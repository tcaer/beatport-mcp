use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rand::random;
use reqwest::Method;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    fs,
    sync::{Mutex, Semaphore},
    task::JoinSet,
};

use crate::{
    AppError, BeatportClient, Result,
    beatport::{ApiResponse, CompactTrackSummary, merge_paging},
};

const DEFAULT_LOOKBACK_DAYS: u64 = 7;
const DEFAULT_MAX_CANDIDATES: u64 = 30;
const DEFAULT_CHARTS_PER_GENRE: u64 = 6;
const SEARCH_PER_PAGE: u64 = 8;
const PLAYLIST_PAGE_SIZE: u64 = 100;
const APPLY_CHUNK_SIZE: usize = 100;
const DEFAULT_SEARCH_CONCURRENCY: usize = 6;
const DUPLICATE_LENGTH_WINDOW_MS: u64 = 4_000;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncCandidate {
    pub artist: String,
    pub track: String,
    pub mix: Option<String>,
    pub genre: Option<String>,
    pub source: String,
    pub source_type: Option<String>,
    pub source_name: Option<String>,
    pub source_url: Option<String>,
    pub discovered_at: Option<String>,
    pub beatport_track_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateBasisKind {
    ExactTrackId,
    Isrc,
    LabelTrackIdentifier,
    NormalizedArtistTitleMix,
    NormalizedArtistTitleHeuristic,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncDuplicateBasis {
    pub kind: DuplicateBasisKind,
    pub matched_track_id: Option<u64>,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncResolvedCandidate {
    pub candidate: SyncCandidate,
    pub matched_track: CompactTrackSummary,
    pub confidence_score: f64,
    pub duplicate_basis: Option<SyncDuplicateBasis>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncAmbiguousCandidate {
    pub candidate: SyncCandidate,
    pub matches: Vec<CompactTrackSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncNotFoundCandidate {
    pub candidate: SyncCandidate,
    pub reason: String,
    pub best_match: Option<CompactTrackSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncFailure {
    pub source: Option<String>,
    pub candidate: Option<SyncCandidate>,
    pub message: String,
    pub path: Option<String>,
    pub status_code: Option<u16>,
    pub retryable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncApplyFailure {
    pub track_ids: Vec<u64>,
    pub message: String,
    pub path: Option<String>,
    pub status_code: Option<u16>,
    pub retryable: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrendingSourceProfile {
    #[default]
    Charts,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncMatchingPolicy {
    #[default]
    Conservative,
    Balanced,
}

impl SyncMatchingPolicy {
    fn minimum_score(self) -> f64 {
        match self {
            Self::Conservative => 80.0,
            Self::Balanced => 68.0,
        }
    }

    fn ambiguity_gap(self) -> f64 {
        match self {
            Self::Conservative => 8.0,
            Self::Balanced => 4.0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyncDedupePolicy {
    PlaylistOnly,
    #[default]
    PlaylistAndSeen,
}

impl SyncDedupePolicy {
    fn uses_seen(self) -> bool {
        matches!(self, Self::PlaylistAndSeen)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TrendingCandidatesRequest {
    pub genres: Vec<String>,
    pub source_profile: Option<TrendingSourceProfile>,
    pub lookback_days: Option<u64>,
    pub max_candidates: Option<u64>,
    pub dedupe_across_genres: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TrendingCandidatesOutput {
    pub source_profile: TrendingSourceProfile,
    pub lookback_days: u64,
    pub genres: Vec<String>,
    pub max_candidates: u64,
    pub dedupe_across_genres: bool,
    pub candidates: Vec<SyncCandidate>,
    pub failures: Vec<SyncFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncDryRunRequest {
    pub playlist_id: u64,
    pub candidates: Vec<SyncCandidate>,
    pub matching_policy: Option<SyncMatchingPolicy>,
    pub dedupe_policy: Option<SyncDedupePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncDryRunSummary {
    pub addable: usize,
    pub already_present: usize,
    pub already_seen: usize,
    pub ambiguous: usize,
    pub not_found: usize,
    pub failures: usize,
    pub remaining_review: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    Ambiguous,
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncReviewItem {
    pub review_index: usize,
    pub candidate: SyncCandidate,
    pub status: ReviewStatus,
    pub options: Vec<CompactTrackSummary>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncDryRunOutput {
    pub plan_id: String,
    pub playlist_id: u64,
    pub matching_policy: SyncMatchingPolicy,
    pub dedupe_policy: SyncDedupePolicy,
    pub summary: PlaylistSyncDryRunSummary,
    pub addable: Vec<SyncResolvedCandidate>,
    pub already_present: Vec<SyncResolvedCandidate>,
    pub already_seen: Vec<SyncResolvedCandidate>,
    pub ambiguous: Vec<SyncAmbiguousCandidate>,
    pub not_found: Vec<SyncNotFoundCandidate>,
    pub review_items: Vec<SyncReviewItem>,
    pub failures: Vec<SyncFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncReviewRequest {
    pub plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncReviewOutput {
    pub plan_id: String,
    pub playlist_id: u64,
    pub summary: PlaylistSyncDryRunSummary,
    pub review_items: Vec<SyncReviewItem>,
    pub already_applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SyncResolutionDecision {
    pub review_index: usize,
    pub chosen_track_id: Option<u64>,
    pub skip: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncResolveRequest {
    pub plan_id: String,
    pub decisions: Vec<SyncResolutionDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncResolveOutput {
    pub plan_id: String,
    pub playlist_id: u64,
    pub summary: PlaylistSyncDryRunSummary,
    pub resolved_to_addable: Vec<SyncResolvedCandidate>,
    pub skipped_review_indices: Vec<usize>,
    pub remaining_review_items: Vec<SyncReviewItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncApplyRequest {
    pub plan_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PlaylistSyncApplyOutput {
    pub plan_id: String,
    pub playlist_id: u64,
    pub added: Vec<CompactTrackSummary>,
    pub skipped_already_present: Vec<CompactTrackSummary>,
    pub skipped_seen: Vec<CompactTrackSummary>,
    pub failed: Vec<SyncApplyFailure>,
    pub applied_at: String,
    pub already_applied: bool,
}

#[derive(Debug, Clone)]
pub struct CrateSyncEngine {
    client: BeatportClient,
    store: SyncStateStore,
    search_limit: usize,
}

impl CrateSyncEngine {
    pub fn new(client: BeatportClient, state_path: PathBuf) -> Self {
        Self {
            search_limit: client
                .sync_search_concurrency()
                .max(DEFAULT_SEARCH_CONCURRENCY.min(client.sync_search_concurrency())),
            client,
            store: SyncStateStore::new(state_path),
        }
    }

    pub async fn trending_candidates(
        &self,
        request: TrendingCandidatesRequest,
    ) -> Result<TrendingCandidatesOutput> {
        let source_profile = request.source_profile.unwrap_or_default();
        if request.genres.is_empty() {
            return Err(AppError::InvalidConfig(
                "genres must contain at least one genre name".into(),
            ));
        }
        let lookback_days = request.lookback_days.unwrap_or(DEFAULT_LOOKBACK_DAYS);
        let max_candidates = request
            .max_candidates
            .unwrap_or(DEFAULT_MAX_CANDIDATES)
            .max(1);
        let dedupe_across_genres = request.dedupe_across_genres.unwrap_or(true);
        let cutoff = Utc::now() - Duration::days(lookback_days as i64);
        let cutoff_date = cutoff.date_naive();
        let genres = self.resolve_genres(&request.genres).await?;
        let per_genre_target = max_candidates.div_ceil(genres.len() as u64).max(1);
        let charts_per_genre = DEFAULT_CHARTS_PER_GENRE.max(per_genre_target.min(10));

        let mut candidates = Vec::new();
        let mut failures = Vec::new();
        let mut seen_track_ids = BTreeSet::new();

        match source_profile {
            TrendingSourceProfile::Charts => {
                for genre in genres {
                    let charts = match self
                        .list_recent_charts_for_genre(genre.id, charts_per_genre, cutoff)
                        .await
                    {
                        Ok(charts) => charts,
                        Err(error) => {
                            failures.push(sync_failure_from_error(
                                Some(genre_source_key(&genre)),
                                None,
                                error,
                                Some("/v4/catalog/charts/".into()),
                            ));
                            continue;
                        }
                    };

                    let mut accepted_for_genre = 0u64;
                    for chart in charts {
                        let tracks = match self.fetch_chart_tracks(chart.id).await {
                            Ok(tracks) => tracks,
                            Err(error) => {
                                failures.push(sync_failure_from_error(
                                    Some(genre_source_key(&genre)),
                                    None,
                                    error,
                                    Some(format!("/v4/catalog/charts/{}/tracks/", chart.id)),
                                ));
                                continue;
                            }
                        };

                        for track in tracks {
                            if accepted_for_genre >= per_genre_target
                                || candidates.len() as u64 >= max_candidates
                            {
                                break;
                            }
                            if !track_is_recent(&track, cutoff_date) {
                                continue;
                            }
                            if dedupe_across_genres && !seen_track_ids.insert(track.id) {
                                continue;
                            }
                            candidates.push(candidate_from_chart_track(&genre, &chart, &track));
                            accepted_for_genre += 1;
                        }

                        if accepted_for_genre >= per_genre_target
                            || candidates.len() as u64 >= max_candidates
                        {
                            break;
                        }
                    }
                }
            }
        }

        Ok(TrendingCandidatesOutput {
            source_profile,
            lookback_days,
            genres: request.genres,
            max_candidates,
            dedupe_across_genres,
            candidates,
            failures,
        })
    }

    pub async fn playlist_sync_dry_run(
        &self,
        request: PlaylistSyncDryRunRequest,
    ) -> Result<PlaylistSyncDryRunOutput> {
        if request.candidates.is_empty() {
            return Err(AppError::InvalidConfig(
                "candidates must contain at least one row".into(),
            ));
        }
        let matching_policy = request.matching_policy.unwrap_or_default();
        let dedupe_policy = request.dedupe_policy.unwrap_or_default();
        let candidates = dedupe_candidates(
            request
                .candidates
                .into_iter()
                .map(normalize_candidate)
                .collect::<Result<Vec<_>>>()?,
        );
        let playlist_tracks = self.fetch_playlist_tracks(request.playlist_id).await?;
        let playlist_index = PlaylistIdentityIndex::from_tracks(&playlist_tracks);
        let seen_state = self.store.snapshot().await?;

        let resolutions = self.resolve_candidates(candidates, matching_policy).await;
        let mut addable = Vec::new();
        let mut already_present = Vec::new();
        let mut already_seen = Vec::new();
        let mut ambiguous = Vec::new();
        let mut not_found = Vec::new();
        let mut failures = Vec::new();
        let mut seen_updates = Vec::new();

        for resolution in resolutions {
            match resolution {
                CandidateResolution::Matched(mut item) => {
                    let seen_key = seen_bucket_key(request.playlist_id, &item.candidate.source);
                    let identity = recording_identity(&item.matched_track);
                    if let Some(basis) = playlist_index.duplicate_basis_for(&item.matched_track) {
                        item.duplicate_basis = Some(basis.clone());
                        seen_updates.push((seen_key, identity.storage_keys()));
                        already_present.push(item);
                    } else if dedupe_policy.uses_seen() {
                        if let Some(basis) = duplicate_basis_from_seen(
                            seen_state.seen.get(&seen_key),
                            &identity,
                            item.matched_track.track_id,
                        ) {
                            item.duplicate_basis = Some(basis.clone());
                            seen_updates.push((seen_key, identity.storage_keys()));
                            already_seen.push(item);
                        } else {
                            seen_updates.push((seen_key, identity.storage_keys()));
                            addable.push(item);
                        }
                    } else {
                        seen_updates.push((seen_key, identity.storage_keys()));
                        addable.push(item);
                    }
                }
                CandidateResolution::Ambiguous(item) => ambiguous.push(item),
                CandidateResolution::NotFound(item) => not_found.push(item),
                CandidateResolution::Failure(item) => failures.push(item),
            }
        }

        let plan_id = new_plan_id();
        let review_items = build_review_items(&ambiguous, &not_found);
        let summary = build_summary(
            addable.len(),
            already_present.len(),
            already_seen.len(),
            ambiguous.len(),
            not_found.len(),
            failures.len(),
            review_items.len(),
        );
        let output = PlaylistSyncDryRunOutput {
            plan_id: plan_id.clone(),
            playlist_id: request.playlist_id,
            matching_policy,
            dedupe_policy,
            summary,
            addable,
            already_present,
            already_seen,
            ambiguous,
            not_found,
            review_items,
            failures,
        };
        self.store
            .record_dry_run(
                output.clone(),
                if dedupe_policy.uses_seen() {
                    seen_updates
                } else {
                    Vec::new()
                },
            )
            .await?;

        Ok(output)
    }

    pub async fn playlist_sync_review(
        &self,
        request: PlaylistSyncReviewRequest,
    ) -> Result<PlaylistSyncReviewOutput> {
        let plan = self
            .store
            .load_plan(&request.plan_id)
            .await?
            .ok_or_else(|| {
                AppError::InvalidConfig(format!("unknown plan_id `{}`", request.plan_id))
            })?;
        Ok(PlaylistSyncReviewOutput {
            plan_id: plan.plan_id.clone(),
            playlist_id: plan.playlist_id,
            summary: plan.summary(),
            review_items: plan.pending_review_items(),
            already_applied: plan.last_apply.is_some(),
        })
    }

    pub async fn playlist_sync_resolve(
        &self,
        request: PlaylistSyncResolveRequest,
    ) -> Result<PlaylistSyncResolveOutput> {
        if request.decisions.is_empty() {
            return Err(AppError::InvalidConfig(
                "decisions must contain at least one resolution".into(),
            ));
        }
        let mut plan = self
            .store
            .load_plan(&request.plan_id)
            .await?
            .ok_or_else(|| {
                AppError::InvalidConfig(format!("unknown plan_id `{}`", request.plan_id))
            })?;
        if plan.last_apply.is_some() {
            return Err(AppError::InvalidConfig(
                "cannot resolve a plan after it has already been applied".into(),
            ));
        }

        let mut resolved_to_addable = Vec::new();
        let mut skipped_review_indices = Vec::new();
        for decision in request.decisions {
            let Some(review_item) = plan
                .review_items
                .iter_mut()
                .find(|item| item.review.review_index == decision.review_index)
            else {
                return Err(AppError::InvalidConfig(format!(
                    "unknown review_index `{}` for plan `{}`",
                    decision.review_index, plan.plan_id
                )));
            };
            if review_item.resolved {
                continue;
            }
            if decision.skip.unwrap_or(false) {
                review_item.resolved = true;
                review_item.skipped = true;
                skipped_review_indices.push(review_item.review.review_index);
                continue;
            }
            let chosen_track_id = decision.chosen_track_id.ok_or_else(|| {
                AppError::InvalidConfig(format!(
                    "review_index `{}` requires chosen_track_id or skip=true",
                    decision.review_index
                ))
            })?;
            let matched_track = review_item
                .review
                .options
                .iter()
                .find(|option| option.track_id == chosen_track_id)
                .cloned()
                .ok_or_else(|| {
                    AppError::InvalidConfig(format!(
                        "chosen_track_id `{chosen_track_id}` is not a valid option for review_index `{}`",
                        decision.review_index
                    ))
                })?;
            let resolved = SyncResolvedCandidate {
                candidate: review_item.review.candidate.clone(),
                matched_track,
                confidence_score: 100.0,
                duplicate_basis: None,
            };
            review_item.resolved = true;
            review_item.skipped = false;
            resolved_to_addable.push(resolved.clone());
            plan.addable.push(resolved);
        }

        self.store.save_plan(plan.clone()).await?;
        Ok(PlaylistSyncResolveOutput {
            plan_id: plan.plan_id.clone(),
            playlist_id: plan.playlist_id,
            summary: plan.summary(),
            resolved_to_addable,
            skipped_review_indices,
            remaining_review_items: plan.pending_review_items(),
        })
    }

    pub async fn playlist_sync_apply(
        &self,
        request: PlaylistSyncApplyRequest,
    ) -> Result<PlaylistSyncApplyOutput> {
        let mut plan = self
            .store
            .load_plan(&request.plan_id)
            .await?
            .ok_or_else(|| {
                AppError::InvalidConfig(format!("unknown plan_id `{}`", request.plan_id))
            })?;
        if let Some(last_apply) = plan.last_apply.clone() {
            return Ok(PlaylistSyncApplyOutput {
                plan_id: plan.plan_id.clone(),
                playlist_id: plan.playlist_id,
                added: last_apply.added,
                skipped_already_present: last_apply.skipped_already_present,
                skipped_seen: last_apply.skipped_seen,
                failed: last_apply.failed,
                applied_at: last_apply.applied_at,
                already_applied: true,
            });
        }

        let playlist_tracks = self.fetch_playlist_tracks(plan.playlist_id).await?;
        let playlist_index = PlaylistIdentityIndex::from_tracks(&playlist_tracks);
        let seen_state = self.store.snapshot().await?;

        let mut to_add = Vec::new();
        let mut skipped_already_present = Vec::new();
        let mut skipped_seen = Vec::new();
        let mut seen_updates = Vec::new();

        for item in &plan.addable {
            let seen_key = seen_bucket_key(plan.playlist_id, &item.candidate.source);
            let identity = recording_identity(&item.matched_track);
            if playlist_index
                .duplicate_basis_for(&item.matched_track)
                .is_some()
            {
                skipped_already_present.push(item.matched_track.clone());
                seen_updates.push((seen_key, identity.storage_keys()));
                continue;
            }
            if plan.dedupe_policy.uses_seen()
                && duplicate_basis_from_seen(
                    seen_state.seen.get(&seen_key),
                    &identity,
                    item.matched_track.track_id,
                )
                .is_some()
            {
                skipped_seen.push(item.matched_track.clone());
                seen_updates.push((seen_key, identity.storage_keys()));
                continue;
            }
            to_add.push(item.matched_track.clone());
            seen_updates.push((seen_key, identity.storage_keys()));
        }

        let mut added = Vec::new();
        let mut failed = Vec::new();
        for chunk in to_add.chunks(APPLY_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let ids = chunk.iter().map(|track| track.track_id).collect::<Vec<_>>();
            match self.add_tracks_to_playlist(plan.playlist_id, &ids).await {
                Ok(()) => added.extend_from_slice(chunk),
                Err(error) => failed.push(apply_failure_from_error(ids, error)),
            }
        }

        let applied_at = Utc::now().to_rfc3339();
        let output = PlaylistSyncApplyOutput {
            plan_id: plan.plan_id.clone(),
            playlist_id: plan.playlist_id,
            added: added.clone(),
            skipped_already_present: skipped_already_present.clone(),
            skipped_seen: skipped_seen.clone(),
            failed: failed.clone(),
            applied_at: applied_at.clone(),
            already_applied: false,
        };
        plan.last_apply = Some(StoredApplySummary {
            applied_at: applied_at.clone(),
            added,
            skipped_already_present,
            skipped_seen,
            failed,
        });
        self.store.record_seen_updates(seen_updates).await?;
        self.store.save_plan(plan).await?;
        Ok(output)
    }

    async fn resolve_genres(&self, inputs: &[String]) -> Result<Vec<ApiGenreEntry>> {
        let all_genres: PaginatedResponse<ApiGenreEntry> = self
            .request_parsed(
                Method::GET,
                "/v4/catalog/genres/",
                Some(&merge_paging(None, Some(1), Some(200))),
                None,
            )
            .await?;
        let mut out = Vec::new();
        for raw in inputs {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(AppError::InvalidConfig(
                    "genre names must not be empty".into(),
                ));
            }
            let found = if let Ok(id) = trimmed.parse::<u64>() {
                all_genres.results.iter().find(|genre| genre.id == id)
            } else {
                let needle = normalize_text(trimmed);
                all_genres.results.iter().find(|genre| {
                    normalize_text(&genre.name) == needle
                        || genre
                            .slug
                            .as_ref()
                            .is_some_and(|slug| normalize_text(slug) == needle)
                })
            };
            let genre = found.ok_or_else(|| {
                AppError::InvalidConfig(format!("unknown Beatport genre `{trimmed}`"))
            })?;
            out.push(genre.clone());
        }
        Ok(out)
    }

    async fn list_recent_charts_for_genre(
        &self,
        genre_id: u64,
        per_page: u64,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<ApiChart>> {
        let mut filters = BTreeMap::new();
        filters.insert("genre_id".into(), json!([genre_id]));
        filters.insert("is_published".into(), json!(true));
        let response: PaginatedResponse<ApiChart> = self
            .request_parsed(
                Method::GET,
                "/v4/catalog/charts/",
                Some(&merge_paging(Some(filters), Some(1), Some(per_page))),
                None,
            )
            .await?;
        Ok(response
            .results
            .into_iter()
            .filter(|chart| chart_is_recent(chart, cutoff))
            .collect())
    }

    async fn fetch_chart_tracks(&self, chart_id: u64) -> Result<Vec<ApiTrack>> {
        let path = format!("/v4/catalog/charts/{chart_id}/tracks/");
        let response: PaginatedResponse<ApiTrack> = self
            .request_parsed(
                Method::GET,
                &path,
                Some(&merge_paging(None, Some(1), Some(PLAYLIST_PAGE_SIZE))),
                None,
            )
            .await?;
        Ok(response.results)
    }

    async fn fetch_playlist_tracks(&self, playlist_id: u64) -> Result<Vec<CompactTrackSummary>> {
        let path = format!("/v4/my/playlists/{playlist_id}/tracks/");
        let mut page = 1;
        let mut tracks = Vec::new();

        loop {
            let response: PaginatedResponse<ApiPlaylistTrackEntry> = self
                .request_parsed(
                    Method::GET,
                    &path,
                    Some(&merge_paging(None, Some(page), Some(PLAYLIST_PAGE_SIZE))),
                    None,
                )
                .await?;

            for entry in response.results {
                if !entry.tombstoned.unwrap_or(false) {
                    tracks.push(compact_track_summary_from_api_track(
                        &entry.track,
                        Some(entry.id),
                    ));
                }
            }

            if response.next.is_none() {
                break;
            }
            page += 1;
        }

        Ok(tracks)
    }

    async fn add_tracks_to_playlist(&self, playlist_id: u64, track_ids: &[u64]) -> Result<()> {
        if track_ids.is_empty() {
            return Ok(());
        }
        let (path, body) = if track_ids.len() == 1 {
            (
                format!("/v4/my/playlists/{playlist_id}/tracks/"),
                json!({ "track_id": track_ids[0] }),
            )
        } else {
            (
                format!("/v4/my/playlists/{playlist_id}/tracks/bulk/"),
                json!({ "track_ids": track_ids }),
            )
        };
        let _response: ApiResponse = self
            .client
            .request(Method::POST, &path, None, Some(&body))
            .await?;
        Ok(())
    }

    async fn resolve_candidates(
        &self,
        candidates: Vec<SyncCandidate>,
        matching_policy: SyncMatchingPolicy,
    ) -> Vec<CandidateResolution> {
        let semaphore = Arc::new(Semaphore::new(self.search_limit.max(1)));
        let mut join_set = JoinSet::new();

        for candidate in candidates {
            let client = self.client.clone();
            let permit_pool = semaphore.clone();
            join_set.spawn(async move {
                let _permit = match permit_pool.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(error) => {
                        return CandidateResolution::Failure(SyncFailure {
                            source: Some(candidate.source.clone()),
                            candidate: Some(candidate.clone()),
                            message: error.to_string(),
                            path: None,
                            status_code: None,
                            retryable: None,
                        });
                    }
                };
                resolve_candidate(client, candidate, matching_policy).await
            });
        }

        let mut out = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(resolution) => out.push(resolution),
                Err(error) => out.push(CandidateResolution::Failure(SyncFailure {
                    source: None,
                    candidate: None,
                    message: error.to_string(),
                    path: None,
                    status_code: None,
                    retryable: None,
                })),
            }
        }
        out
    }

    async fn request_parsed<T>(
        &self,
        method: Method,
        path: &str,
        query: Option<&BTreeMap<String, Value>>,
        body: Option<&Value>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = self.client.request(method, path, query, body).await?;
        serde_json::from_value(response.data).map_err(AppError::from)
    }
}

#[derive(Debug, Clone)]
enum CandidateResolution {
    Matched(SyncResolvedCandidate),
    Ambiguous(SyncAmbiguousCandidate),
    NotFound(SyncNotFoundCandidate),
    Failure(SyncFailure),
}

async fn resolve_candidate(
    client: BeatportClient,
    candidate: SyncCandidate,
    matching_policy: SyncMatchingPolicy,
) -> CandidateResolution {
    if let Some(track_id) = candidate.beatport_track_id {
        return CandidateResolution::Matched(SyncResolvedCandidate {
            matched_track: CompactTrackSummary {
                track_id,
                playlist_track_id: None,
                artist_names: split_artists(&candidate.artist),
                track_name: candidate.track.clone(),
                mix_name: candidate.mix.clone(),
                isrc: None,
                label_track_identifier: None,
                genre: candidate.genre.clone(),
                length_ms: None,
                bpm: None,
                release_name: None,
                publish_date: None,
                url: None,
            },
            candidate,
            confidence_score: 100.0,
            duplicate_basis: None,
        });
    }

    let query = build_search_query(&candidate);
    let mut filters = BTreeMap::new();
    filters.insert("q".into(), json!(query));
    filters.insert("per_page".into(), json!(SEARCH_PER_PAGE));

    let search: ApiSearchResults = match client
        .request(Method::GET, "/v4/catalog/search/", Some(&filters), None)
        .await
    {
        Ok(response) => match serde_json::from_value(response.data) {
            Ok(search) => search,
            Err(error) => {
                return CandidateResolution::Failure(sync_failure_from_error(
                    Some(candidate.source.clone()),
                    Some(candidate),
                    error.into(),
                    Some("/v4/catalog/search/".into()),
                ));
            }
        },
        Err(error) => {
            return CandidateResolution::Failure(sync_failure_from_error(
                Some(candidate.source.clone()),
                Some(candidate),
                error,
                Some("/v4/catalog/search/".into()),
            ));
        }
    };

    let mut scored = search
        .tracks
        .unwrap_or_default()
        .into_iter()
        .map(|track| score_candidate_match(&candidate, track))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if scored.is_empty() {
        return CandidateResolution::NotFound(SyncNotFoundCandidate {
            candidate,
            reason: "no search results".into(),
            best_match: None,
        });
    }

    let mut collapsed = Vec::new();
    let mut seen_identities = BTreeSet::new();
    for scored_match in scored {
        let identity_key = recording_identity(&scored_match.summary).primary_key();
        if seen_identities.insert(identity_key) {
            collapsed.push(scored_match);
        }
    }

    let best = &collapsed[0];
    if !best.accepts(matching_policy) {
        return CandidateResolution::NotFound(SyncNotFoundCandidate {
            candidate,
            reason: "no confident match".into(),
            best_match: Some(best.summary.clone()),
        });
    }

    let near_matches = collapsed
        .iter()
        .take(5)
        .filter(|candidate_score| {
            best.score - candidate_score.score <= matching_policy.ambiguity_gap()
        })
        .map(|candidate_score| candidate_score.summary.clone())
        .collect::<Vec<_>>();
    if near_matches.len() > 1 {
        return CandidateResolution::Ambiguous(SyncAmbiguousCandidate {
            candidate,
            matches: near_matches,
        });
    }

    CandidateResolution::Matched(SyncResolvedCandidate {
        candidate,
        matched_track: best.summary.clone(),
        confidence_score: best.score,
        duplicate_basis: None,
    })
}

fn build_search_query(candidate: &SyncCandidate) -> String {
    let mut parts = vec![candidate.artist.clone(), candidate.track.clone()];
    if let Some(mix) = candidate.mix.as_ref() {
        parts.push(mix.clone());
    }
    parts.join(" ")
}

fn dedupe_candidates(candidates: Vec<SyncCandidate>) -> Vec<SyncCandidate> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for candidate in candidates {
        let key = candidate_dedupe_key(&candidate);
        if seen.insert(key) {
            out.push(candidate);
        }
    }
    out
}

fn candidate_dedupe_key(candidate: &SyncCandidate) -> String {
    if let Some(track_id) = candidate.beatport_track_id {
        return format!("track:{track_id}");
    }
    format!(
        "{}|{}|{}|{}",
        normalize_text(&candidate.artist),
        normalize_text(&candidate.track),
        candidate
            .mix
            .as_deref()
            .map(normalize_text)
            .unwrap_or_default(),
        candidate
            .genre
            .as_deref()
            .map(normalize_text)
            .unwrap_or_default()
    )
}

fn normalize_candidate(candidate: SyncCandidate) -> Result<SyncCandidate> {
    let artist = candidate.artist.trim().to_string();
    let track = candidate.track.trim().to_string();
    let source = candidate.source.trim().to_string();
    if artist.is_empty() {
        return Err(AppError::InvalidConfig(
            "candidate artist must not be empty".into(),
        ));
    }
    if track.is_empty() {
        return Err(AppError::InvalidConfig(
            "candidate track must not be empty".into(),
        ));
    }
    if source.is_empty() {
        return Err(AppError::InvalidConfig(
            "candidate source must not be empty".into(),
        ));
    }
    Ok(SyncCandidate {
        artist,
        track,
        mix: trim_optional(candidate.mix),
        genre: trim_optional(candidate.genre),
        source: source.clone(),
        source_type: trim_optional(candidate.source_type),
        source_name: trim_optional(candidate.source_name).or_else(|| Some(source.clone())),
        source_url: trim_optional(candidate.source_url),
        discovered_at: trim_optional(candidate.discovered_at),
        beatport_track_id: candidate.beatport_track_id,
    })
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn normalize_text(text: &str) -> String {
    let lower = text.to_lowercase();
    let replaced = lower.replace('’', "'").replace('&', " and ");
    let without_parens = strip_parenthetical(&replaced);
    without_parens
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_title(text: &str) -> String {
    let normalized = normalize_text(text);
    for marker in [" feat ", " ft ", " featuring "] {
        if let Some(index) = normalized.find(marker) {
            return normalized[..index].trim().to_string();
        }
    }
    normalized
}

fn strip_parenthetical(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut depth = 0usize;
    for ch in text.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(ch),
            _ => {}
        }
    }
    out
}

fn normalize_artist_list(text: &str) -> Vec<String> {
    text.split(',')
        .flat_map(|chunk| {
            chunk
                .split('/')
                .flat_map(|part| part.split('&'))
                .map(|part| normalize_text(part.trim()))
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn split_artists(text: &str) -> Vec<String> {
    normalize_artist_list(text)
}

fn token_overlap(left: &str, right: &str) -> usize {
    let left_tokens = left
        .split_whitespace()
        .map(|token| token.to_string())
        .collect::<BTreeSet<_>>();
    let right_tokens = right
        .split_whitespace()
        .map(|token| token.to_string())
        .collect::<BTreeSet<_>>();
    left_tokens.intersection(&right_tokens).count()
}

fn score_candidate_match(candidate: &SyncCandidate, track: ApiTrack) -> ScoredMatch {
    let candidate_title = normalize_title(&candidate.track);
    let candidate_mix = candidate
        .mix
        .as_deref()
        .map(normalize_text)
        .unwrap_or_default();
    let candidate_genre = candidate.genre.as_deref().map(normalize_text);
    let candidate_artists = normalize_artist_list(&candidate.artist);

    let track_title = normalize_title(&track.name);
    let track_mix = track
        .mix_name
        .as_deref()
        .map(normalize_text)
        .unwrap_or_default();
    let track_genre = track
        .genre
        .as_ref()
        .map(|genre| normalize_text(&genre.name));
    let track_artists = track
        .artists
        .iter()
        .map(|artist| normalize_text(&artist.name))
        .collect::<Vec<_>>();

    let mut score = 0.0;
    let mut title_signal = false;
    let mut artist_signal = false;

    if candidate_title == track_title {
        score += 60.0;
        title_signal = true;
    } else if !candidate_title.is_empty()
        && !track_title.is_empty()
        && (candidate_title.contains(&track_title) || track_title.contains(&candidate_title))
    {
        score += 42.0;
        title_signal = true;
    } else {
        let overlap = token_overlap(&candidate_title, &track_title);
        if overlap >= 2 {
            score += (overlap as f64 * 10.0).min(28.0);
            title_signal = true;
        }
    }

    let mut artist_overlap = 0usize;
    for artist in &candidate_artists {
        if track_artists.iter().any(|track_artist| {
            artist == track_artist || artist.contains(track_artist) || track_artist.contains(artist)
        }) {
            artist_overlap += 1;
        }
    }
    if artist_overlap > 0 {
        artist_signal = true;
        score += (25.0 + (artist_overlap as f64 * 6.0)).min(36.0);
    } else {
        score -= 6.0;
    }

    if !candidate_mix.is_empty() {
        if candidate_mix == track_mix {
            score += 18.0;
        } else {
            let overlap = token_overlap(&candidate_mix, &track_mix);
            if overlap >= 1 {
                score += (overlap as f64 * 6.0).min(10.0);
            }
            if candidate_mix.contains("remix") && !track_mix.contains("remix") {
                score -= 10.0;
            }
        }
    }

    if let (Some(candidate_genre), Some(track_genre)) = (candidate_genre, track_genre) {
        if candidate_genre == track_genre {
            score += 10.0;
        } else {
            score -= 6.0;
        }
    }

    if track.is_available_for_streaming.unwrap_or(false) {
        score += 0.5;
    }

    if let Some(candidate_mix) = candidate.mix.as_ref().map(|mix| normalize_text(mix)) {
        if candidate_mix.contains("extended") {
            if track_mix.contains("extended") || track_mix.contains("club") {
                score += 4.0;
            }
        }
    }

    ScoredMatch {
        summary: compact_track_summary_from_api_track(&track, None),
        score,
        title_signal,
        artist_signal,
    }
}

fn candidate_from_chart_track(
    genre: &ApiGenreEntry,
    chart: &ApiChart,
    track: &ApiTrack,
) -> SyncCandidate {
    SyncCandidate {
        artist: join_artist_names(&track.artists),
        track: track.name.clone(),
        mix: track.mix_name.clone(),
        genre: track.genre.as_ref().map(|item| item.name.clone()),
        source: genre_source_key(genre),
        source_type: Some("beatport_chart".into()),
        source_name: chart.name.clone().or_else(|| Some(genre.name.clone())),
        source_url: chart.url.clone(),
        discovered_at: Some(Utc::now().to_rfc3339()),
        beatport_track_id: Some(track.id),
    }
}

fn genre_source_key(genre: &ApiGenreEntry) -> String {
    format!(
        "beatport_charts:{}",
        genre
            .slug
            .clone()
            .unwrap_or_else(|| normalize_text(&genre.name))
    )
}

fn join_artist_names(artists: &[ApiArtist]) -> String {
    artists
        .iter()
        .map(|artist| artist.name.clone())
        .collect::<Vec<_>>()
        .join(", ")
}

fn compact_track_summary_from_api_track(
    track: &ApiTrack,
    playlist_track_id: Option<u64>,
) -> CompactTrackSummary {
    CompactTrackSummary {
        track_id: track.id,
        playlist_track_id,
        artist_names: track
            .artists
            .iter()
            .map(|artist| artist.name.clone())
            .collect(),
        track_name: track.name.clone(),
        mix_name: track.mix_name.clone(),
        isrc: track.isrc.clone(),
        label_track_identifier: track.label_track_identifier.clone(),
        genre: track.genre.as_ref().map(|genre| genre.name.clone()),
        length_ms: track
            .length_ms
            .or_else(|| parse_length_to_ms(track.length.as_ref())),
        bpm: track.bpm,
        release_name: track.release.as_ref().map(|release| release.name.clone()),
        publish_date: track
            .new_release_date
            .clone()
            .or_else(|| track.publish_date.clone()),
        url: track.url.clone(),
    }
}

fn parse_length_to_ms(length: Option<&Value>) -> Option<u64> {
    match length {
        Some(Value::Number(number)) => number.as_u64().map(|seconds| seconds * 1000),
        Some(Value::String(text)) => {
            let parts = text
                .split(':')
                .filter_map(|part| part.parse::<u64>().ok())
                .collect::<Vec<_>>();
            match parts.as_slice() {
                [minutes, seconds] => Some((minutes * 60 + seconds) * 1000),
                [hours, minutes, seconds] => Some((hours * 3600 + minutes * 60 + seconds) * 1000),
                _ => None,
            }
        }
        _ => None,
    }
}

fn chart_is_recent(chart: &ApiChart, cutoff: DateTime<Utc>) -> bool {
    chart
        .publish_date
        .as_deref()
        .and_then(parse_datetime)
        .map(|publish_date| publish_date >= cutoff)
        .unwrap_or(true)
}

fn track_is_recent(track: &ApiTrack, cutoff_date: NaiveDate) -> bool {
    track
        .new_release_date
        .as_deref()
        .and_then(parse_date)
        .or_else(|| {
            track
                .publish_date
                .as_deref()
                .and_then(parse_datetime)
                .map(|value| value.date_naive())
        })
        .map(|publish_date| publish_date >= cutoff_date)
        .unwrap_or(true)
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn parse_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn seen_bucket_key(playlist_id: u64, source: &str) -> String {
    format!("{playlist_id}:{}", normalize_text(source))
}

fn new_plan_id() -> String {
    format!(
        "syncplan_{}_{}",
        Utc::now().format("%Y%m%d%H%M%S"),
        format!("{:016x}", random::<u64>())
    )
}

fn build_summary(
    addable: usize,
    already_present: usize,
    already_seen: usize,
    ambiguous: usize,
    not_found: usize,
    failures: usize,
    remaining_review: usize,
) -> PlaylistSyncDryRunSummary {
    PlaylistSyncDryRunSummary {
        addable,
        already_present,
        already_seen,
        ambiguous,
        not_found,
        failures,
        remaining_review,
    }
}

fn build_review_items(
    ambiguous: &[SyncAmbiguousCandidate],
    not_found: &[SyncNotFoundCandidate],
) -> Vec<SyncReviewItem> {
    let mut out = Vec::new();
    for item in ambiguous {
        out.push(SyncReviewItem {
            review_index: out.len() + 1,
            candidate: item.candidate.clone(),
            status: ReviewStatus::Ambiguous,
            options: item.matches.iter().take(5).cloned().collect(),
            reason: None,
        });
    }
    for item in not_found {
        let mut options = Vec::new();
        if let Some(best_match) = item.best_match.clone() {
            options.push(best_match);
        }
        out.push(SyncReviewItem {
            review_index: out.len() + 1,
            candidate: item.candidate.clone(),
            status: ReviewStatus::NotFound,
            options,
            reason: Some(item.reason.clone()),
        });
    }
    out
}

#[derive(Debug, Clone)]
struct RecordingIdentity {
    track_id: u64,
    isrc: Option<String>,
    label_track_identifier: Option<String>,
    artist_title_mix: String,
    artist_title: String,
    length_ms: Option<u64>,
    bpm: Option<u64>,
}

impl RecordingIdentity {
    fn primary_key(&self) -> String {
        if let Some(isrc) = self.isrc.as_ref() {
            return format!("isrc:{isrc}");
        }
        if let Some(label) = self.label_track_identifier.as_ref() {
            return format!("label:{label}");
        }
        format!("mix:{}", self.artist_title_mix)
    }

    fn storage_keys(&self) -> Vec<String> {
        let mut keys = vec![
            format!("track:{}", self.track_id),
            format!("mix:{}", self.artist_title_mix),
        ];
        keys.push(format!("title:{}", self.artist_title));
        if let Some(isrc) = self.isrc.as_ref() {
            keys.push(format!("isrc:{isrc}"));
        }
        if let Some(label) = self.label_track_identifier.as_ref() {
            keys.push(format!("label:{label}"));
        }
        keys.sort();
        keys.dedup();
        keys
    }
}

fn recording_identity(summary: &CompactTrackSummary) -> RecordingIdentity {
    let artists = if summary.artist_names.is_empty() {
        String::new()
    } else {
        summary.artist_names.join(" ")
    };
    let artist_title = format!(
        "{}|{}",
        normalize_text(&artists),
        normalize_title(&summary.track_name)
    );
    let artist_title_mix = format!(
        "{}|{}",
        artist_title,
        summary
            .mix_name
            .as_deref()
            .map(normalize_text)
            .unwrap_or_default()
    );
    RecordingIdentity {
        track_id: summary.track_id,
        isrc: summary.isrc.as_deref().map(normalize_text),
        label_track_identifier: summary
            .label_track_identifier
            .as_deref()
            .map(normalize_text),
        artist_title_mix,
        artist_title,
        length_ms: summary.length_ms,
        bpm: summary.bpm,
    }
}

fn duplicate_basis_from_seen(
    seen: Option<&BTreeSet<String>>,
    identity: &RecordingIdentity,
    track_id: u64,
) -> Option<SyncDuplicateBasis> {
    let seen = seen?;
    if seen.contains(&format!("track:{track_id}")) {
        return Some(SyncDuplicateBasis {
            kind: DuplicateBasisKind::ExactTrackId,
            matched_track_id: Some(track_id),
            detail: "matched previously seen exact Beatport track id".into(),
        });
    }
    if let Some(isrc) = identity.isrc.as_ref()
        && seen.contains(&format!("isrc:{isrc}"))
    {
        return Some(SyncDuplicateBasis {
            kind: DuplicateBasisKind::Isrc,
            matched_track_id: None,
            detail: format!("matched previously seen ISRC `{isrc}`"),
        });
    }
    if let Some(label) = identity.label_track_identifier.as_ref()
        && seen.contains(&format!("label:{label}"))
    {
        return Some(SyncDuplicateBasis {
            kind: DuplicateBasisKind::LabelTrackIdentifier,
            matched_track_id: None,
            detail: format!("matched previously seen label track identifier `{label}`"),
        });
    }
    if seen.contains(&format!("mix:{}", identity.artist_title_mix)) {
        return Some(SyncDuplicateBasis {
            kind: DuplicateBasisKind::NormalizedArtistTitleMix,
            matched_track_id: None,
            detail: "matched previously seen normalized artist/title/mix".into(),
        });
    }
    if seen.contains(&format!("title:{}", identity.artist_title)) {
        return Some(SyncDuplicateBasis {
            kind: DuplicateBasisKind::NormalizedArtistTitleHeuristic,
            matched_track_id: None,
            detail: "matched previously seen normalized artist/title".into(),
        });
    }
    None
}

#[derive(Debug, Clone)]
struct PlaylistIdentityIndex {
    tracks: Vec<CompactTrackSummary>,
}

impl PlaylistIdentityIndex {
    fn from_tracks(tracks: &[CompactTrackSummary]) -> Self {
        Self {
            tracks: tracks.to_vec(),
        }
    }

    fn duplicate_basis_for(&self, candidate: &CompactTrackSummary) -> Option<SyncDuplicateBasis> {
        let candidate_identity = recording_identity(candidate);
        for track in &self.tracks {
            let identity = recording_identity(track);
            if track.track_id == candidate.track_id {
                return Some(SyncDuplicateBasis {
                    kind: DuplicateBasisKind::ExactTrackId,
                    matched_track_id: Some(track.track_id),
                    detail: "matched exact Beatport track id already in playlist".into(),
                });
            }
            if let (Some(left), Some(right)) =
                (candidate_identity.isrc.as_ref(), identity.isrc.as_ref())
                && left == right
            {
                return Some(SyncDuplicateBasis {
                    kind: DuplicateBasisKind::Isrc,
                    matched_track_id: Some(track.track_id),
                    detail: format!("matched existing playlist track by ISRC `{left}`"),
                });
            }
            if let (Some(left), Some(right)) = (
                candidate_identity.label_track_identifier.as_ref(),
                identity.label_track_identifier.as_ref(),
            ) && left == right
            {
                return Some(SyncDuplicateBasis {
                    kind: DuplicateBasisKind::LabelTrackIdentifier,
                    matched_track_id: Some(track.track_id),
                    detail: format!(
                        "matched existing playlist track by label track identifier `{left}`"
                    ),
                });
            }
            if candidate_identity.artist_title_mix == identity.artist_title_mix {
                return Some(SyncDuplicateBasis {
                    kind: DuplicateBasisKind::NormalizedArtistTitleMix,
                    matched_track_id: Some(track.track_id),
                    detail: "matched existing playlist track by normalized artist/title/mix".into(),
                });
            }
            if candidate_identity.artist_title == identity.artist_title
                && close_length(candidate_identity.length_ms, identity.length_ms)
                && close_bpm(candidate_identity.bpm, identity.bpm)
            {
                return Some(SyncDuplicateBasis {
                    kind: DuplicateBasisKind::NormalizedArtistTitleHeuristic,
                    matched_track_id: Some(track.track_id),
                    detail: "matched existing playlist track by normalized artist/title with similar duration/BPM".into(),
                });
            }
        }
        None
    }
}

fn close_length(left: Option<u64>, right: Option<u64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.abs_diff(right) <= DUPLICATE_LENGTH_WINDOW_MS,
        _ => true,
    }
}

fn close_bpm(left: Option<u64>, right: Option<u64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.abs_diff(right) <= 1,
        _ => true,
    }
}

#[derive(Debug, Clone)]
struct ScoredMatch {
    summary: CompactTrackSummary,
    score: f64,
    title_signal: bool,
    artist_signal: bool,
}

impl ScoredMatch {
    fn accepts(&self, policy: SyncMatchingPolicy) -> bool {
        self.title_signal && self.artist_signal && self.score >= policy.minimum_score()
    }
}

#[derive(Debug, Clone)]
struct SyncStateStore {
    path: PathBuf,
    cache: Arc<Mutex<Option<SyncStateFile>>>,
}

impl SyncStateStore {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    async fn snapshot(&self) -> Result<SyncStateFile> {
        let mut guard = self.cache.lock().await;
        self.ensure_loaded(&mut guard).await?;
        Ok(guard.clone().unwrap_or_default())
    }

    async fn record_dry_run(
        &self,
        output: PlaylistSyncDryRunOutput,
        seen_updates: Vec<(String, Vec<String>)>,
    ) -> Result<()> {
        let mut guard = self.cache.lock().await;
        self.ensure_loaded(&mut guard).await?;
        let state = guard.as_mut().expect("state should be loaded");
        for (key, identities) in seen_updates {
            let bucket = state.seen.entry(key).or_default();
            for identity in identities {
                bucket.insert(identity);
            }
        }
        state
            .plans
            .insert(output.plan_id.clone(), StoredSyncPlan::from_output(output));
        self.persist(state).await
    }

    async fn record_seen_updates(&self, seen_updates: Vec<(String, Vec<String>)>) -> Result<()> {
        let mut guard = self.cache.lock().await;
        self.ensure_loaded(&mut guard).await?;
        let state = guard.as_mut().expect("state should be loaded");
        for (key, identities) in seen_updates {
            let bucket = state.seen.entry(key).or_default();
            for identity in identities {
                bucket.insert(identity);
            }
        }
        self.persist(state).await
    }

    async fn load_plan(&self, plan_id: &str) -> Result<Option<StoredSyncPlan>> {
        let mut guard = self.cache.lock().await;
        self.ensure_loaded(&mut guard).await?;
        Ok(guard
            .as_ref()
            .and_then(|state| state.plans.get(plan_id).cloned()))
    }

    async fn save_plan(&self, plan: StoredSyncPlan) -> Result<()> {
        let mut guard = self.cache.lock().await;
        self.ensure_loaded(&mut guard).await?;
        let state = guard.as_mut().expect("state should be loaded");
        state.plans.insert(plan.plan_id.clone(), plan);
        self.persist(state).await
    }

    async fn ensure_loaded(&self, guard: &mut Option<SyncStateFile>) -> Result<()> {
        if guard.is_some() {
            return Ok(());
        }
        let loaded = match fs::read_to_string(&self.path).await {
            Ok(contents) => serde_json::from_str::<SyncStateFile>(&contents)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => SyncStateFile::default(),
            Err(error) => return Err(error.into()),
        };
        *guard = Some(loaded);
        Ok(())
    }

    async fn persist(&self, state: &SyncStateFile) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let bytes = serde_json::to_vec_pretty(state)?;
        fs::write(&self.path, bytes).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct SyncStateFile {
    version: u8,
    seen: BTreeMap<String, BTreeSet<String>>,
    plans: BTreeMap<String, StoredSyncPlan>,
}

impl Default for SyncStateFile {
    fn default() -> Self {
        Self {
            version: 2,
            seen: BTreeMap::new(),
            plans: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredReviewItem {
    review: SyncReviewItem,
    resolved: bool,
    skipped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSyncPlan {
    plan_id: String,
    playlist_id: u64,
    created_at: String,
    matching_policy: SyncMatchingPolicy,
    dedupe_policy: SyncDedupePolicy,
    addable: Vec<SyncResolvedCandidate>,
    already_present: Vec<SyncResolvedCandidate>,
    already_seen: Vec<SyncResolvedCandidate>,
    ambiguous: Vec<SyncAmbiguousCandidate>,
    not_found: Vec<SyncNotFoundCandidate>,
    review_items: Vec<StoredReviewItem>,
    failures: Vec<SyncFailure>,
    last_apply: Option<StoredApplySummary>,
}

impl StoredSyncPlan {
    fn from_output(output: PlaylistSyncDryRunOutput) -> Self {
        Self {
            plan_id: output.plan_id,
            playlist_id: output.playlist_id,
            created_at: Utc::now().to_rfc3339(),
            matching_policy: output.matching_policy,
            dedupe_policy: output.dedupe_policy,
            addable: output.addable,
            already_present: output.already_present,
            already_seen: output.already_seen,
            ambiguous: output.ambiguous,
            not_found: output.not_found,
            review_items: output
                .review_items
                .into_iter()
                .map(|review| StoredReviewItem {
                    review,
                    resolved: false,
                    skipped: false,
                })
                .collect(),
            failures: output.failures,
            last_apply: None,
        }
    }

    fn pending_review_items(&self) -> Vec<SyncReviewItem> {
        self.review_items
            .iter()
            .filter(|item| !item.resolved)
            .map(|item| item.review.clone())
            .collect()
    }

    fn summary(&self) -> PlaylistSyncDryRunSummary {
        build_summary(
            self.addable.len(),
            self.already_present.len(),
            self.already_seen.len(),
            self.ambiguous.len(),
            self.not_found.len(),
            self.failures.len(),
            self.review_items
                .iter()
                .filter(|item| !item.resolved)
                .count(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredApplySummary {
    applied_at: String,
    added: Vec<CompactTrackSummary>,
    skipped_already_present: Vec<CompactTrackSummary>,
    skipped_seen: Vec<CompactTrackSummary>,
    failed: Vec<SyncApplyFailure>,
}

#[derive(Debug, Clone, Deserialize)]
struct PaginatedResponse<T> {
    next: Option<String>,
    results: Vec<T>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiSearchResults {
    tracks: Option<Vec<ApiTrack>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiPlaylistTrackEntry {
    id: u64,
    tombstoned: Option<bool>,
    track: ApiTrack,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiChart {
    id: u64,
    name: Option<String>,
    url: Option<String>,
    publish_date: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiGenreEntry {
    id: u64,
    name: String,
    slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiTrack {
    id: u64,
    name: String,
    mix_name: Option<String>,
    genre: Option<ApiGenre>,
    artists: Vec<ApiArtist>,
    release: Option<ApiRelease>,
    url: Option<String>,
    publish_date: Option<String>,
    new_release_date: Option<String>,
    is_available_for_streaming: Option<bool>,
    isrc: Option<String>,
    label_track_identifier: Option<String>,
    length_ms: Option<u64>,
    length: Option<Value>,
    bpm: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiGenre {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiRelease {
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiArtist {
    name: String,
}

fn sync_failure_from_error(
    source: Option<String>,
    candidate: Option<SyncCandidate>,
    error: AppError,
    path: Option<String>,
) -> SyncFailure {
    match error {
        AppError::BeatportApi {
            status, message, ..
        } => SyncFailure {
            source,
            candidate,
            message,
            path,
            status_code: Some(status),
            retryable: Some(status == 429 || status >= 500),
        },
        AppError::Http(error) => SyncFailure {
            source,
            candidate,
            message: error.to_string(),
            path,
            status_code: None,
            retryable: Some(error.is_timeout() || error.is_connect() || error.is_request()),
        },
        other => SyncFailure {
            source,
            candidate,
            message: other.to_string(),
            path,
            status_code: None,
            retryable: None,
        },
    }
}

fn apply_failure_from_error(track_ids: Vec<u64>, error: AppError) -> SyncApplyFailure {
    match error {
        AppError::BeatportApi {
            status, message, ..
        } => SyncApplyFailure {
            track_ids,
            message,
            path: None,
            status_code: Some(status),
            retryable: Some(status == 429 || status >= 500),
        },
        AppError::Http(error) => SyncApplyFailure {
            track_ids,
            message: error.to_string(),
            path: None,
            status_code: None,
            retryable: Some(error.is_timeout() || error.is_connect() || error.is_request()),
        },
        other => SyncApplyFailure {
            track_ids,
            message: other.to_string(),
            path: None,
            status_code: None,
            retryable: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn candidate() -> SyncCandidate {
        SyncCandidate {
            artist: "Mau P".into(),
            track: "The Less I Know The Better".into(),
            mix: Some("Extended Mix".into()),
            genre: Some("Tech House".into()),
            source: "beatport_charts:tech-house".into(),
            source_type: Some("beatport_chart".into()),
            source_name: Some("Tech House Top 10".into()),
            source_url: None,
            discovered_at: None,
            beatport_track_id: None,
        }
    }

    fn track(id: u64, artist: &str, name: &str, mix: &str, genre: &str) -> ApiTrack {
        ApiTrack {
            id,
            name: name.into(),
            mix_name: Some(mix.into()),
            genre: Some(ApiGenre { name: genre.into() }),
            artists: vec![ApiArtist {
                name: artist.into(),
            }],
            release: Some(ApiRelease {
                name: "Release".into(),
            }),
            url: Some(format!("https://example.com/tracks/{id}")),
            publish_date: Some("2026-03-28".into()),
            new_release_date: Some("2026-03-28".into()),
            is_available_for_streaming: Some(true),
            isrc: Some(format!("ISRC{id}")),
            label_track_identifier: Some(format!("LBL-{id}")),
            length_ms: Some(367_000),
            length: None,
            bpm: Some(128),
        }
    }

    #[test]
    fn exact_match_scores_higher_than_loose_match() {
        let exact = score_candidate_match(
            &candidate(),
            track(
                1,
                "Mau P",
                "The Less I Know The Better",
                "Extended Mix",
                "Tech House",
            ),
        );
        let loose = score_candidate_match(
            &candidate(),
            track(
                2,
                "Somebody Else",
                "The Less I Know",
                "Original Mix",
                "House",
            ),
        );
        assert!(exact.score > loose.score);
        assert!(exact.accepts(SyncMatchingPolicy::Conservative));
    }

    #[test]
    fn near_equal_scores_are_ambiguous() {
        let exact = score_candidate_match(
            &candidate(),
            track(
                1,
                "Mau P",
                "The Less I Know The Better",
                "Extended Mix",
                "Tech House",
            ),
        );
        let alt = score_candidate_match(
            &candidate(),
            track(
                2,
                "Mau P",
                "The Less I Know The Better",
                "Extended Mix",
                "Tech House",
            ),
        );
        assert!(exact.score - alt.score <= SyncMatchingPolicy::Conservative.ambiguity_gap());
    }

    #[test]
    fn dedupes_duplicate_candidate_rows_by_track_id_or_normalized_text() {
        let mut duplicate = candidate();
        duplicate.mix = Some(" extended mix ".into());
        let deduped = dedupe_candidates(vec![candidate(), duplicate]);
        assert_eq!(deduped.len(), 1);
    }

    #[test]
    fn semantic_duplicate_prefers_isrc_before_heuristics() {
        let existing = compact_track_summary_from_api_track(
            &track(
                1,
                "Mau P",
                "The Less I Know The Better",
                "Original Mix",
                "Tech House",
            ),
            Some(10),
        );
        let mut incoming = existing.clone();
        incoming.track_id = 99;
        let basis = PlaylistIdentityIndex::from_tracks(&[existing])
            .duplicate_basis_for(&incoming)
            .expect("duplicate basis should exist");
        assert_eq!(basis.kind, DuplicateBasisKind::Isrc);
    }

    #[tokio::test]
    async fn state_store_persists_seen_tracks_and_plans() {
        let dir = tempdir().expect("tempdir should exist");
        let store = SyncStateStore::new(dir.path().join("sync-state.json"));
        let output = PlaylistSyncDryRunOutput {
            plan_id: "plan-1".into(),
            playlist_id: 42,
            matching_policy: SyncMatchingPolicy::Conservative,
            dedupe_policy: SyncDedupePolicy::PlaylistAndSeen,
            summary: build_summary(1, 0, 0, 0, 0, 0, 0),
            addable: vec![SyncResolvedCandidate {
                candidate: candidate(),
                matched_track: CompactTrackSummary {
                    track_id: 100,
                    playlist_track_id: None,
                    artist_names: vec!["Mau P".into()],
                    track_name: "The Less I Know The Better".into(),
                    mix_name: Some("Extended Mix".into()),
                    isrc: Some("ABC123".into()),
                    label_track_identifier: Some("LBL-100".into()),
                    genre: Some("Tech House".into()),
                    length_ms: Some(367_000),
                    bpm: Some(128),
                    release_name: Some("Release".into()),
                    url: None,
                    publish_date: None,
                },
                confidence_score: 99.0,
                duplicate_basis: None,
            }],
            already_present: Vec::new(),
            already_seen: Vec::new(),
            ambiguous: Vec::new(),
            not_found: Vec::new(),
            review_items: Vec::new(),
            failures: Vec::new(),
        };
        store
            .record_dry_run(
                output.clone(),
                vec![(
                    seen_bucket_key(42, &output.addable[0].candidate.source),
                    recording_identity(&output.addable[0].matched_track).storage_keys(),
                )],
            )
            .await
            .expect("dry run should persist");

        let snapshot = store.snapshot().await.expect("snapshot should load");
        assert!(
            snapshot
                .seen
                .get(&seen_bucket_key(42, &output.addable[0].candidate.source))
                .expect("seen bucket should exist")
                .contains("track:100")
        );
        assert!(snapshot.plans.contains_key("plan-1"));
    }
}
