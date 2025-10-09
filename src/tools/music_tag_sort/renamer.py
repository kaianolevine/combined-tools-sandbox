import os
import re
from typing import Dict
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.mp4 import MP4
from mutagen.easyid3 import EasyID3
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def new_sanitize_filename(value):
    # Replace any non-alphanumeric or underscore character with underscore
    value = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    return value


def sanitize_filename(value):
    value = re.sub(r"\s+", "_", value)
    return re.sub(r"[^a-zA-Z0-9_\-]", "", value)


def get_metadata(file_path):
    ext = file_path.lower().split(".")[-1]
    audio = None
    if ext == "mp3":
        audio = MP3(file_path, ID3=EasyID3)
    elif ext == "flac":
        audio = FLAC(file_path)
    elif ext in ("m4a", "mp4"):
        audio = MP4(file_path)
    else:
        raise ValueError(f"Unsupported file format: {ext}")

    tags = audio.tags or {}
    artist = tags.get("artist", ["Unknown"])[0]
    title = tags.get("title", ["Unknown"])[0]
    bpm_raw = tags.get("bpm", [""])[0]
    try:
        bpm = str(int(round(float(bpm_raw))))
    except (ValueError, TypeError):
        bpm = ""
    comment = tags.get("comment", [""])[0]

    return {
        "artist": artist,
        "title": title,
        "bpm": str(bpm),
        "comment": str(comment),
    }


def rename_music_file(file_path, output_dir):
    metadata = get_metadata(file_path)
    filename_parts = [
        metadata["bpm"],
        metadata["title"],
        metadata["artist"],
        metadata["comment"],
    ]
    cleaned_parts = [sanitize_filename(p) for p in filename_parts if p]
    new_filename = "__".join(cleaned_parts) + os.path.splitext(file_path)[1]
    new_path = os.path.join(output_dir, new_filename)
    logging.debug(f"Renaming {file_path} to {new_path}")
    os.rename(file_path, new_path)
    return new_path


def rename_music_files(directory):
    for filename in os.listdir(directory):
        full_path = os.path.join(directory, filename)
        if os.path.isfile(full_path):
            metadata = get_metadata(full_path)
            if metadata:
                logging.debug(f"Renaming {full_path} using metadata: {metadata}")
                title, artist, bpm = metadata
                new_name = f"{bpm}__{sanitize_filename(title)}__{sanitize_filename(artist)}.mp3"
                new_path = os.path.join(directory, new_name)
                os.rename(full_path, new_path)


def rename_files_in_directory(directory: str, config: Dict) -> None:
    logging.info(f"Scanning directory: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            try:
                full_path = os.path.join(root, file)
                if not os.path.isfile(full_path):
                    continue
                metadata = get_metadata(full_path)
                new_name = generate_filename(metadata, config)
                if not new_name:
                    logging.warning(f"Skipping file due to missing metadata: {file}")
                    continue
                new_path = os.path.join(root, new_name)
                if new_path != full_path:
                    logging.debug(f"Renaming file: {file} -> {new_name}")
                    os.rename(full_path, new_path)
                    logging.info(f"Renamed: {file} -> {new_name}")
            except Exception as e:
                logging.error(f"Failed to rename file: {file}", exc_info=True)
                logging.error(f"Failed to rename file: {file}. Error: {e}")


def generate_filename(metadata: Dict[str, str], config: Dict) -> str:
    """
    Generate a sanitized filename based on selected metadata fields and config-defined order.

    Args:
        metadata: A dictionary with keys like 'bpm', 'title', 'artist', 'comment'.
        config: A dictionary with a key 'order' specifying the field order for filenames.

    Returns:
        A string filename with sanitized fields, joined by '__', or None if required fields are missing.
    """
    logging.debug(f"Generating filename using metadata: {metadata} and config: {config}")
    filename_parts = []
    for field in config.get("rename_order", []):
        value = metadata.get(field, "")
        logging.debug(f"Field: {field}, Value: {value}")
        if not value and field in config.get("required_fields", []):
            logging.debug(f"Required field '{field}' is missing, skipping filename generation.")
            return None  # skip if a required field is missing
        sanitized = sanitize_filename(value)
        if sanitized:
            filename_parts.append(sanitized)
    if not filename_parts:
        logging.debug("No valid fields found for filename generation, returning None.")
        return None
    logging.debug(f"Generated filename: {'__'.join(filename_parts)}")
    return "__".join(filename_parts) + config.get("extension", ".mp3")
