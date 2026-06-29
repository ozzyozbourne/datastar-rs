use bytes::Bytes;

#[cfg(feature = "compression")]
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionStrategy {
    ServerPriority,
    ClientPriority,
    Forced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    Brotli,
    Zstd,
    Gzip,
    Deflate,
}

impl CompressionAlgorithm {
    pub const fn encoding(self) -> &'static str {
        match self {
            Self::Brotli => "br",
            Self::Zstd => "zstd",
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compression {
    pub strategy: CompressionStrategy,
    pub algorithms: Vec<CompressionAlgorithm>,
    pub accept_encoding: Option<String>,
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            strategy: CompressionStrategy::ServerPriority,
            algorithms: vec![
                CompressionAlgorithm::Brotli,
                CompressionAlgorithm::Zstd,
                CompressionAlgorithm::Gzip,
                CompressionAlgorithm::Deflate,
            ],
            accept_encoding: None,
        }
    }
}

impl Compression {
    pub fn accept_encoding(mut self, accept_encoding: impl Into<String>) -> Self {
        self.accept_encoding = Some(accept_encoding.into());
        self
    }

    pub fn strategy(mut self, strategy: CompressionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn algorithms(
        mut self,
        algorithms: impl IntoIterator<Item = CompressionAlgorithm>,
    ) -> Self {
        self.algorithms = algorithms.into_iter().collect();
        self
    }

    pub fn selected_algorithm(&self) -> Option<CompressionAlgorithm> {
        match self.strategy {
            CompressionStrategy::Forced => self.algorithms.first().copied(),
            CompressionStrategy::ServerPriority => {
                let accepted = parse_accept_encoding(self.accept_encoding.as_deref().unwrap_or(""));
                self.algorithms
                    .iter()
                    .copied()
                    .find(|algorithm| accepted.iter().any(|a| a == algorithm.encoding()))
            }
            CompressionStrategy::ClientPriority => {
                let accepted = parse_accept_encoding(self.accept_encoding.as_deref().unwrap_or(""));
                accepted.iter().find_map(|accepted| {
                    self.algorithms
                        .iter()
                        .copied()
                        .find(|algorithm| algorithm.encoding() == accepted)
                })
            }
        }
    }
}

pub(crate) fn parse_accept_encoding(header: &str) -> Vec<String> {
    header
        .split(',')
        .filter_map(|part| part.trim().split(';').next())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(feature = "compression")]
pub(crate) fn compress_chunk(
    algorithm: CompressionAlgorithm,
    bytes: &Bytes,
) -> Result<Bytes, std::io::Error> {
    match algorithm {
        CompressionAlgorithm::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes)?;
            Ok(Bytes::from(encoder.finish()?))
        }
        CompressionAlgorithm::Deflate => {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes)?;
            Ok(Bytes::from(encoder.finish()?))
        }
        CompressionAlgorithm::Brotli => {
            let mut out = Vec::new();
            {
                let mut encoder = brotli::CompressorWriter::new(&mut out, 4096, 6, 22);
                encoder.write_all(bytes)?;
            }
            Ok(Bytes::from(out))
        }
        CompressionAlgorithm::Zstd => {
            let out = zstd::stream::encode_all(bytes.as_ref(), 0)?;
            Ok(Bytes::from(out))
        }
    }
}

#[cfg(not(feature = "compression"))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn compress_chunk(
    _algorithm: CompressionAlgorithm,
    bytes: &Bytes,
) -> Result<Bytes, std::io::Error> {
    Ok(bytes.clone())
}
