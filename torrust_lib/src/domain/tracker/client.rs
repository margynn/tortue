use super::{AnnounceRequest, TrackerResponse};

pub(crate) trait TrackerAnnouncer {
    async fn announce(&self, req: &AnnounceRequest) -> Option<TrackerResponse>;
}
