use mp3lame_encoder::{Builder, Encoder, FlushNoGap, InterleavedPcm};

/// MP3 encoder wrapper around mp3lame-encoder
pub struct Mp3Encoder {
    encoder: Encoder,
    mp3_buffer: Vec<u8>,
}

impl Mp3Encoder {
    pub fn new(sample_rate: u32, channels: u32, bitrate: u32) -> Self {
        let mut builder = Builder::new().expect("failed to create LAME builder");
        builder
            .set_sample_rate(sample_rate)
            .expect("set sample rate");
        builder
            .set_num_channels(channels as u8)
            .expect("set channels");
        builder
            .set_quality(mp3lame_encoder::Quality::Best)
            .expect("set quality");

        let brate = match bitrate {
            128 => mp3lame_encoder::Bitrate::Kbps128,
            160 => mp3lame_encoder::Bitrate::Kbps160,
            192 => mp3lame_encoder::Bitrate::Kbps192,
            256 => mp3lame_encoder::Bitrate::Kbps256,
            320 => mp3lame_encoder::Bitrate::Kbps320,
            _ => mp3lame_encoder::Bitrate::Kbps192,
        };
        builder.set_brate(brate).expect("set bitrate");

        let encoder = builder.build().expect("failed to build LAME encoder");
        let mp3_buffer = Vec::with_capacity(16384);

        Self {
            encoder,
            mp3_buffer,
        }
    }

    /// Encode interleaved S16 PCM samples to MP3
    pub fn encode(&mut self, samples: &[i16]) -> Vec<u8> {
        let input = InterleavedPcm(samples);

        // Ensure buffer has enough capacity for worst case
        let needed = mp3lame_encoder::max_required_buffer_size(samples.len());
        self.mp3_buffer.clear();
        self.mp3_buffer.reserve(needed);
        match self.encoder.encode_to_vec(input, &mut self.mp3_buffer) {
            Ok(_size) => self.mp3_buffer.clone(),
            Err(e) => {
                log::error!("MP3 encode error: {:?}", e);
                Vec::new()
            }
        }
    }

    /// Flush the encoder (call at end of track or on stop)
    pub fn flush(&mut self) -> Vec<u8> {
        self.mp3_buffer.clear();
        match self
            .encoder
            .flush_to_vec::<FlushNoGap>(&mut self.mp3_buffer)
        {
            Ok(_size) => self.mp3_buffer.clone(),
            Err(e) => {
                log::error!("MP3 flush error: {:?}", e);
                Vec::new()
            }
        }
    }
}

/// Generate MP3 silence (for Content-Length padding and Station buffer reset)
pub fn generate_silence(
    duration_ms: u32,
    sample_rate: u32,
    channels: u32,
    bitrate: u32,
) -> Vec<u8> {
    let mut encoder = Mp3Encoder::new(sample_rate, channels, bitrate);
    let num_samples =
        (sample_rate as usize * channels as usize * duration_ms as usize) / 1000;
    let silence_pcm = vec![0i16; num_samples];
    let mut result = encoder.encode(&silence_pcm);
    result.extend(encoder.flush());
    result
}
