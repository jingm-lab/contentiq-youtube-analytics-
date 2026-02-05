from collections import defaultdict


def apply_diversity_constraint(candidates, max_per_channel=3, top_n=10):
    """Apply channel diversity constraint to candidate videos.

    Args:
        candidates (Dataframe): Dataframe with columns (video_id, channel_id, distance, title)
        max_per_channel (int): Maximum videos to include from any single channel. Defaults to 3.

    Returns filtered Dataframe with diversity constraint applied.
    """
    row_idx_to_keep = []
    count_dict = defaultdict(int)

    for row in candidates.itertuples():
        channel_id = row.channel_id
        if len(row_idx_to_keep) < top_n:
            if count_dict[channel_id] < max_per_channel:
                row_idx_to_keep.append(row.Index)
                count_dict[channel_id] += 1
        else:
            break
    return candidates.iloc[row_idx_to_keep]
