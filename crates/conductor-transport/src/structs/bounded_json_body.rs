use super::*;

pub(super) struct BoundedJsonBody(pub(super) Vec<u8>);

impl Write for BoundedJsonBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let new_len = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_TRANSPORT_BODY_BYTES)
            .ok_or_else(|| io::Error::other("transport request exceeds body limit"))?;
        self.0
            .try_reserve(new_len - self.0.len())
            .map_err(io::Error::other)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
