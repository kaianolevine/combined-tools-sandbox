# Converted from Google Apps Script to Python

import time
import re

# Constants
DJ_SETS = "11zVwUZLDfB6uXpwNdA3c4Xsev2aG26fc"  # My Drive/Deejay Marvel/DJ Sets
CSV_FILES = "1YskZ8sD2H0bA9rxzWnE8iV15P7kWRk8N"  # My Drive/Deejay Marvel/CSV-Uploaded
ARCHIVE_FOLDER_NAME = "csvs"
MAX_FILES = 20
desiredOrder = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM", "Length"]

# Simulated in-memory locking mechanism (should be replaced with persistent store in prod)
_folder_locks = {}


def try_lock_folder(folder_name):
    """Try to acquire a lock for a specific folder name."""
    now = time.time() * 1000
    expires_in = 7 * 60 * 1000  # 7 minutes
    key = f"LOCK_{folder_name}"
    existing = _folder_locks.get(key)
    if existing and now - existing < expires_in:
        return False
    _folder_locks[key] = now
    return True


def release_folder_lock(folder_name):
    """Release the lock for a specific folder name."""
    key = f"LOCK_{folder_name}"
    _folder_locks.pop(key, None)


def clean_title(value):
    """Remove parenthetical phrases from a title string (e.g., '(Remix)')."""
    return re.sub(r"\s*\([^)]*\)", "", str(value or "")).strip()


def levenshtein_distance(a, b):
    """Compute Levenshtein edit distance between two strings."""
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
    return dp[m][n]


def string_similarity(a, b):
    """Calculate normalized string similarity between two strings."""
    if not a or not b:
        return 0
    d = levenshtein_distance(a, b)
    return 1 - d / max(len(a), len(b))


def get_shared_filled_fields(row_a, row_b, dedup_indices):
    """Count how many deduplication fields are filled in both rows."""
    return sum(
        1
        for dedup in dedup_indices
        if str(row_a[dedup["index"]] if row_a[dedup["index"]] is not None else "").strip()
        and str(row_b[dedup["index"]] if row_b[dedup["index"]] is not None else "").strip()
    )


def get_dedup_match_score(row_a, row_b, dedup_indices):
    """Evaluate similarity score across deduplication fields."""
    total = 0
    matches = 0
    for dedup in dedup_indices:
        field = dedup["field"]
        index = dedup["index"]
        a = str(row_a[index] if row_a[index] is not None else "").strip().lower()
        b = str(row_b[index] if row_b[index] is not None else "").strip().lower()
        if not a or not b:
            matches += 1
            total += 1
        else:
            total += 1
            if string_similarity(a, b) >= 0.5:
                matches += 1
            elif field == "Title" and clean_title(a) == clean_title(b):
                matches += 1
    return matches / total if total > 0 else 0


def get_or_create_subfolder(parent, name):
    """Placeholder for Drive API get-or-create subfolder logic."""
    # This should be implemented with the actual Drive API client
    raise NotImplementedError("Drive API subfolder management not implemented.")
