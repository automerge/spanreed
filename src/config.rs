use crate::{share_policy::ShareAll, storage::InMemory, SharePolicy, Storage};

pub struct RepoConfig<Store: Storage, Share: SharePolicy> {
    storage: Store,
    share: Share,
}

impl<Store: Storage, Share: SharePolicy> RepoConfig<Store, Share> {
    pub fn new(storage: Store, share: Share) -> Self {
        Self { storage, share }
    }
}

/// Create a builder for a `RepoConfig`. The builder will initially use an in-memory store and
/// a policy which shares all documents with all peers.
pub fn builder() -> ConfigBuilder<InMemory, ShareAll> {
    ConfigBuilder {
        store: InMemory::new(),
        share: ShareAll::new(),
    }
}

pub struct ConfigBuilder<Store: Storage, Share: SharePolicy> {
    store: Store,
    share: Share,
}

impl<Store: Storage, Share: SharePolicy> ConfigBuilder<Store, Share> {
    pub fn build(self) -> RepoConfig<Store, Share> {
        RepoConfig {
            storage: self.store,
            share: self.share,
        }
    }

    pub fn with_share<Share2: SharePolicy>(self, share: Share2) -> ConfigBuilder<Store, Share2> {
        ConfigBuilder {
            store: self.store,
            share,
        }
    }

    pub fn with_storage<Store2: Storage>(self, storage: Store2) -> ConfigBuilder<Store2, Share> {
        ConfigBuilder {
            store: storage,
            share: self.share,
        }
    }
}
