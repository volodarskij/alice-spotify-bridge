use std::sync::Arc;
use std::time::{Duration, Instant};

use librespot_playback::audio_backend::{Sink, SinkResult};
use librespot_playback::config::AudioFormat;
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use parking_lot::Mutex;

use crate::encoder::Mp3Encoder;
use crate::state::{BridgeShared, SAMPLE_RATE, CHANNELS};

/// Custom librespot Sink with real-time rate limiting.
///
/// Rate limits PCM production to ~real-time + 500ms pre-buffer.
/// Progress bar offset (~3s) is inherent to the architecture:
/// librespot position = decoded samples, Station plays with ~3s buffer delay.
/// Same as Bluetooth/Chromecast speakers.
pub struct BridgeSink {
    encoder: Mp3Encoder,
    shared: Arc<Mutex<BridgeShared>>,
    _format: AudioFormat,
    start_time: Instant,
    samples_written: u64,
}

impl BridgeSink {
    pub fn new(shared: Arc<Mutex<BridgeShared>>, format: AudioFormat) -> Self {
        Self {
            encoder: Mp3Encoder::new(SAMPLE_RATE, CHANNELS, crate::state::MP3_BITRATE),
            shared,
            _format: format,
            start_time: Instant::now(),
            samples_written: 0,
        }
    }
}

impl Sink for BridgeSink {
    fn start(&mut self) -> SinkResult<()> {
        log::info!("BridgeSink: start (new encoder)");
        self.encoder = Mp3Encoder::new(SAMPLE_RATE, CHANNELS, crate::state::MP3_BITRATE);
        self.start_time = Instant::now();
        self.samples_written = 0;
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        log::info!("BridgeSink: stop (flushing encoder)");
        let remaining = self.encoder.flush();
        if !remaining.is_empty() {
            let mut shared = self.shared.lock();
            shared.push_mp3(remaining);
        }
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        {
            let shared = self.shared.lock();
            if !shared.streaming {
                return Ok(());
            }
        }

        let samples = match packet {
            AudioPacket::Samples(ref s) => converter.f64_to_s16(s),
            AudioPacket::Raw(_) => return Ok(()),
        };

        // Rate limiting: keep PCM max 500ms ahead of wall clock
        let stereo_samples = samples.len() as u64 / CHANNELS as u64;
        self.samples_written += stereo_samples;
        let audio_ms = (self.samples_written * 1000) / SAMPLE_RATE as u64;
        let wall_ms = self.start_time.elapsed().as_millis() as u64;

        if audio_ms > wall_ms + 500 {
            let sleep_ms = (audio_ms - wall_ms - 200).min(200);
            std::thread::sleep(Duration::from_millis(sleep_ms));
        }

        let mp3_data = self.encoder.encode(&samples);

        if !mp3_data.is_empty() {
            let mut shared = self.shared.lock();
            shared.push_mp3(mp3_data);
        }

        Ok(())
    }
}
