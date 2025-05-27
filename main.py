import os
import uuid
import shutil
import requests
import logging
import re
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
import json
from pydantic import BaseModel
from moviepy.editor import (
    VideoFileClip,
    AudioFileClip,
    concatenate_videoclips,
    CompositeAudioClip,
)
import requests
from upload_google import upload_video


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create necessary directories
TEMP_DIR = "temp"
OUTPUT_DIR = "output"

# Get absolute paths
TEMP_DIR = os.path.abspath(TEMP_DIR)
OUTPUT_DIR = os.path.abspath(OUTPUT_DIR)

# Create directories
logger.info(f"Creating directories: TEMP_DIR={TEMP_DIR}, OUTPUT_DIR={OUTPUT_DIR}")
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
logger.info(f"Directories created successfully")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
import os
import subprocess


def add_subtitle_to_video(video_path, request_temp_dir, audio_path):
    """
    Add subtitle to a video using FFmpeg.

    :param video_path: Path to the input video file
    :param request_temp_dir: Directory to store temporary files
    :param merge_data: Dictionary containing subtitle properties
    :return: Path to the output video with subtitles
    """

    # Verify that the video file exists
    if not os.path.exists(video_path):
        print(f"Error: Video file '{video_path}' not found.")
        return None

    # Ensure the temp directory exists
    if not os.path.exists(request_temp_dir):
        os.makedirs(request_temp_dir)

    files = {"myfile": open(audio_path, "rb")}
    data = {
        "params": json.dumps(
            {
                "template_id": "23fec5329d9b49beb5f1d4bb26b367c2",
                "auth": {"key": "1248e507bec74296933469de5a477ee2"},
            }
        )
    }

    response = requests.post(
        "https://api2.transloadit.com/assemblies", files=files, data=data
    )
    if response.status_code == 200:
        print("API call successful:", response.json())

    import time

    assembly_ssl_url = response.json().get("assembly_ssl_url")
    if assembly_ssl_url:
        while True:
            poll_response = requests.get(assembly_ssl_url)
            poll_data = poll_response.json()
            if (
                poll_data.get("ok") == "ASSEMBLY_COMPLETED"
                and poll_response.status_code == 200
            ):
                print("The Assembly was successfully completed.", poll_response)
                # Extract the SRT file URL from the assembly result
                transcribed_results = poll_data.get("results", {}).get(
                    "transcribed", []
                )
                srt_url = None
                if transcribed_results and len(transcribed_results) > 0:
                    srt_url = transcribed_results[0].get("ssl_url")

                # Download the SRT file if the URL is available
                if srt_url:
                    srt_response = requests.get(srt_url)
                    if srt_response.status_code == 200:
                        srt_path = os.path.join(request_temp_dir, "subtitle.srt")
                        with open(srt_path, "wb") as srt_file:
                            srt_file.write(srt_response.content)
                        print("Downloaded SRT file successfully:", srt_path)
                    else:
                        print(
                            "Failed to download SRT file, HTTP status:",
                            srt_response.status_code,
                        )
                else:
                    print("No SRT URL found in the assembly result.")
                break
            else:
                print(f"Polling for assembly completion...", poll_data)
                time.sleep(10)  # Poll every 5 seconds
    else:
        print("No assembly URL found in the response.")

    subtitle_file = os.path.join(request_temp_dir, "subtitle.srt")

    # # Create SRT file with subtitles, breaking the subtitle_text into parts
    # if "subtitle_text" in merge_data and merge_data["subtitle_text"]:
    #     # Split the subtitle_text by periods to create separate sentences
    #     raw_subtitles = merge_data["subtitle_text"].split(".")
    #     # Clean up the sentences and remove empty ones
    #     subtitles = [subtitle.strip() for subtitle in raw_subtitles if subtitle.strip()]
    #     # Add periods back to the end of each subtitle if not already present
    #     subtitles = [
    #         subtitle + "." if not subtitle.endswith(".") else subtitle
    #         for subtitle in subtitles
    #     ]
    # else:
    #     # Fallback to default subtitles if no subtitle_text is provided
    #     subtitles = [
    #         "Embark on a journey of timeless wisdom.",
    #         "From ancient Greece to modern life.",
    #         "In the shadow of Titans, we discover eternal truths etched in marble.",
    #         "Through ancient eyes, we see the present anew, a canvas of choices and change.",
    #         "The gods whisper through marble, guiding us toward unyielding resolve amid chaos.",
    #         "In contemplation, find the courage to act; in action, find the peace of mind.",
    #         "These lessons endure as marble, teaching us strength, patience, and enduring wisdom.",
    #     ]

    # with open(subtitle_file, "w") as f:
    #     for i, subtitle in enumerate(subtitles):
    #         start_time = f"00:00:{i*4:02},000"
    #         end_time = f"00:00:{(i+1)*4:02},000"
    #         f.write(f"{i+1}\n{start_time} --> {end_time}\n{subtitle}\n\n")

    # Add subtitle to video using FFmpeg
    output_video_with_subtitle = os.path.join(
        request_temp_dir, "video_with_subtitle.mp4"
    )
    ffmpeg_cmd = [
        "ffmpeg",
        "-i",
        video_path,
        "-vf",
        f"subtitles={subtitle_file}:force_style='FontName=Montserrat SemiBold,Fontsize={merge_data['subtitle_font_size']},Alignment=10,PrimaryColour=&H80FFFFFF,BorderStyle=0.5,OutlineColour=&H00000000,Shadow=1,ShadowColour=&H20000000,MarginV=0,MarginL=0,MarginR=0,Bold=0'",
        "-c:v",
        "libx264",
        output_video_with_subtitle,
    ]

    try:
        subprocess.run(ffmpeg_cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to add subtitle: {e}")
        return None

    return output_video_with_subtitle


class MergeRequest(BaseModel):
    video_urls: List[str]
    background_audio_url: Optional[str] = None
    background_volume: Optional[float] = 0.5
    upload_to_drive: Optional[bool] = False
    subtitle_text: Optional[str] = None
    subtitle_font_size: Optional[int] = 24
    subtitle_color: Optional[str] = "white"


def extract_drive_file_id(url):
    """Extract file ID from Google Drive URL"""
    logger.info(f"Extracting Drive file ID from URL: {url}")

    # Simple pattern for extracting ID from /file/d/{id}/ format
    simple_pattern = r"/file/d/([\w-]+)"
    simple_match = re.search(simple_pattern, url)
    if simple_match:
        file_id = simple_match.group(1)
        logger.info(f"Found file ID using simple pattern: {file_id}")
        return file_id

    # Pattern for direct download URLs: https://drive.google.com/uc?id={fileid}&export=download
    direct_pattern = r"[?&]id=([\w-]+)"
    direct_match = re.search(direct_pattern, url)
    if direct_match:
        file_id = direct_match.group(1)
        logger.info(f"Found file ID from direct download URL: {file_id}")
        return file_id

    # Pattern for https://drive.google.com/file/d/{fileid}/view
    file_id_match = re.search(r"drive\.google\.com/file/d/([^/]+)", url) or re.search(
        r"drive.google.com/file/d/([^/]+)", url
    )
    if file_id_match:
        file_id = file_id_match.group(1)
        logger.info(f"Found file ID using drive pattern: {file_id}")
        return file_id

    # Pattern for https://docs.google.com/document/d/{fileid}/edit
    docs_match = re.search(r"docs\.google\.com/\w+/d/([^/]+)", url) or re.search(
        r"docs.google.com/\w+/d/([^/]+)", url
    )
    if docs_match:
        file_id = docs_match.group(1)
        logger.info(f"Found file ID using docs pattern: {file_id}")
        return file_id

    # Direct file ID
    if re.match(r"^[A-Za-z0-9_-]{25,}$", url):
        logger.info(f"URL appears to be a direct file ID: {url}")
        return url

    logger.error(f"Could not extract file ID from URL: {url}")
    return None


def download_drive_file(url, output_path):
    """Download a file from Google Drive"""
    try:
        file_id = extract_drive_file_id(url)
        if not file_id:
            logger.error(f"Could not extract file ID from Google Drive URL: {url}")
            return False

        # Construct direct download URL
        download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
        logger.info(
            f"Downloading Google Drive file with ID {file_id} from {download_url}"
        )

        # First request to get the confirmation token for large files
        session = requests.Session()
        response = session.get(download_url, stream=True, timeout=30)
        response.raise_for_status()

        # Check if this is a large file that requires confirmation
        for key, value in response.cookies.items():
            if key.startswith("download_warning"):
                logger.info("Large file detected, adding confirmation parameter")
                # Add confirmation parameter
                download_url = f"{download_url}&confirm={value}"
                break

        logger.info(f"Final download URL: {download_url}")

        # Download the file
        with open(output_path, "wb") as f:
            response = session.get(download_url, stream=True, timeout=30)
            response.raise_for_status()

            # Check content type to make sure we're getting a file and not an HTML page
            content_type = response.headers.get("Content-Type", "")
            if "html" in content_type.lower() and not content_type.startswith(
                ("audio", "video", "application")
            ):
                logger.error(
                    f"Received HTML content instead of file. Content-Type: {content_type}"
                )
                return False

            # Download the file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # Verify file was downloaded successfully
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logger.info(
                f"Successfully downloaded Google Drive file to {output_path} ({os.path.getsize(output_path)} bytes)"
            )
            return True
        else:
            logger.error(f"Downloaded file is empty or does not exist: {output_path}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error when downloading Google Drive file: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error downloading Google Drive file: {str(e)}")
        return False


def download_file(url, output_path):
    """Download a file from URL to the specified path with validation"""
    try:
        logger.info(f"Downloading file from {url} to {output_path}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors

        content_length = response.headers.get("content-length")
        if content_length is None:
            logger.warning(f"Content-Length header missing for {url}")
        else:
            logger.info(f"Expected file size: {content_length} bytes")

        with open(output_path, "wb") as f:
            downloaded_size = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    downloaded_size += len(chunk)

        # Verify file was downloaded and has content
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            actual_size = os.path.getsize(output_path)
            logger.info(f"Downloaded file size: {actual_size} bytes")

            # Verify file size matches content-length if available
            if content_length and int(content_length) > 0:
                if (
                    abs(int(content_length) - actual_size) > 100
                ):  # Allow small difference
                    logger.warning(
                        f"File size mismatch: expected {content_length}, got {actual_size}"
                    )

            # Basic validation for video files
            if output_path.endswith((".mp4", ".avi", ".mov", ".mkv")):
                try:
                    import subprocess

                    # Use ffprobe to check if file is a valid video
                    cmd = [
                        "ffprobe",
                        "-v",
                        "error",
                        "-select_streams",
                        "v:0",
                        "-show_entries",
                        "stream=codec_type",
                        "-of",
                        "csv=p=0",
                        output_path,
                    ]
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if "video" not in result.stdout.strip():
                        logger.error(
                            f"Downloaded file is not a valid video: {output_path}"
                        )
                        return False
                except Exception as e:
                    logger.warning(
                        f"Could not validate video file {output_path}: {str(e)}"
                    )

            return True
        else:
            logger.error(f"Downloaded file is empty or does not exist: {output_path}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading file from {url}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading file from {url}: {str(e)}")
        return False


def cleanup_files(file_paths):
    """Clean up temporary files with robust error handling"""
    logger.info(f"Cleaning up files: {file_paths}")
    for file_path in file_paths:
        if os.path.exists(file_path):
            # Try multiple times in case of locked files
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    if os.path.isdir(file_path):
                        logger.info(
                            f"Removing directory: {file_path} (attempt {attempt+1}/{max_attempts})"
                        )
                        # Use ignore_errors to handle permission issues
                        shutil.rmtree(file_path, ignore_errors=True)
                    else:
                        logger.info(
                            f"Removing file: {file_path} (attempt {attempt+1}/{max_attempts})"
                        )
                        # Check if file is still in use
                        try:
                            # Try to open the file to see if it's locked
                            with open(file_path, "a"):
                                pass
                        except IOError:
                            logger.warning(f"File appears to be locked: {file_path}")
                            # Wait a moment before retrying
                            import time

                            time.sleep(1)
                            continue

                        # Try to remove the file
                        os.remove(file_path)

                    # If we get here without exception, removal was successful
                    logger.info(f"Successfully removed: {file_path}")
                    break
                except Exception as e:
                    logger.error(
                        f"Error removing {file_path} (attempt {attempt+1}/{max_attempts}): {str(e)}"
                    )
                    if attempt < max_attempts - 1:
                        # Wait before retrying
                        import time

                        time.sleep(1)
                    else:
                        logger.error(
                            f"Failed to remove {file_path} after {max_attempts} attempts"
                        )
        else:
            logger.info(f"File or directory does not exist, skipping: {file_path}")


def cleanup_all_temp_output():
    """Clean up all files in temp and output directories with robust error handling"""
    logger.info("Cleaning up all files in temp and output directories")

    # Get all files and directories to clean up
    items_to_clean = []

    # Add temp directory items
    if os.path.exists(TEMP_DIR):
        for item in os.listdir(TEMP_DIR):
            items_to_clean.append(os.path.join(TEMP_DIR, item))

    # Add output directory items
    if os.path.exists(OUTPUT_DIR):
        for item in os.listdir(OUTPUT_DIR):
            items_to_clean.append(os.path.join(OUTPUT_DIR, item))

    # Use our improved cleanup_files function to handle the cleanup
    if items_to_clean:
        logger.info(f"Found {len(items_to_clean)} items to clean up")
        cleanup_files(items_to_clean)
    else:
        logger.info("No files found to clean up")

    # Verify cleanup was successful
    temp_files_remaining = (
        os.listdir(TEMP_DIR)
        if os.path.exists(TEMP_DIR) and os.path.isdir(TEMP_DIR)
        else []
    )
    output_files_remaining = (
        os.listdir(OUTPUT_DIR)
        if os.path.exists(OUTPUT_DIR) and os.path.isdir(OUTPUT_DIR)
        else []
    )

    if not temp_files_remaining and not output_files_remaining:
        logger.info("All directories successfully cleaned")
    else:
        logger.warning(
            f"Some files could not be removed: {len(temp_files_remaining)} in temp, {len(output_files_remaining)} in output"
        )

    # Ensure directories exist for future use
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


class MergeVideoRequest(BaseModel):
    video_urls: List[str]
    background_audio_url: Optional[str] = None
    background_volume: Optional[float] = 0.5
    upload_to_drive: Optional[bool] = False
    subtitle_data: Optional[dict] = {
        "subtitle_text": "",
        "subtitle_font_size": 24,
        "subtitle_color": "white",
    }


@app.post("/merge")
async def merge_videos(
    background_tasks: BackgroundTasks,
    request: MergeVideoRequest,
    narration_file: UploadFile = None,
):
    """Endpoint for JSON requests without file uploads"""
    return await process_merge_request(background_tasks, request, None)


@app.post("/merge-form")
async def merge_videos_form(
    background_tasks: BackgroundTasks,
    video_urls: str = Form(...),
    background_audio_url: str = Form(None),
    background_volume: float = Form(0.5),
    upload_to_drive: bool = Form(False),
    subtitle_text: str = Form(None),
    subtitle_font_size: int = Form(24),
    subtitle_color: str = Form("white"),
    narration_file: str = Form(None),
):
    """Endpoint for form data requests with file uploads"""
    # Convert form data to MergeVideoRequest object
    try:
        # Parse video_urls JSON string to list
        video_urls_list = json.loads(video_urls)

        # Create subtitle data dictionary
        subtitle_data = {
            "subtitle_text": subtitle_text,
            "subtitle_font_size": subtitle_font_size,
            "subtitle_color": subtitle_color,
        }

        # Create request object
        request = MergeVideoRequest(
            video_urls=video_urls_list,
            background_audio_url=background_audio_url,
            background_volume=background_volume,
            upload_to_drive=upload_to_drive,
            subtitle_data=subtitle_data,
        )

        return await process_merge_request(background_tasks, request, narration_file)
    except json.JSONDecodeError:
        return {"error": "Invalid JSON in video_urls"}


async def process_merge_request(
    background_tasks: BackgroundTasks,
    request: MergeVideoRequest,
    narration_file: str = None,
    narration_url: str = None,
):
    logger.info("Merge endpoint accessed")
    logger.info(f"Received request: {request}")

    # Extract video URLs from the request
    video_urls = request.video_urls
    if not video_urls:
        logger.error("No video URLs provided in the request")
        return {"error": "No video URLs provided"}

    # Get upload_to_drive parameter
    upload_to_drive_bool = request.upload_to_drive

    # Get background audio URL and volume
    background_audio_url = request.background_audio_url
    background_volume = request.background_volume

    # Get subtitle data
    subtitle_data = request.subtitle_data

    logger.info(f"Using request data: {request}")

    # Create a unique ID for this request
    request_id = str(uuid.uuid4())
    logger.info(f"Generated request ID: {request_id}")
    request_temp_dir = os.path.join(TEMP_DIR, request_id)
    logger.info(f"Creating temp directory: {request_temp_dir}")
    os.makedirs(request_temp_dir, exist_ok=True)

    # List to track files for cleanup - only include temporary directory
    files_to_cleanup = [request_temp_dir]

    try:
        # Download videos
        video_paths = []
        for i, video_url in enumerate(video_urls):
            video_path = os.path.join(request_temp_dir, f"video_{i}.mp4")
            logger.info(f"Downloading video {i+1}/{len(video_urls)} from {video_url}")
            if download_file(video_url, video_path):
                video_paths.append(video_path)
            else:
                logger.error(f"Failed to download video from {video_url}")
                return {"error": f"Failed to download video {i+1} from {video_url}"}

        if not video_paths:
            logger.error("No videos were successfully downloaded")
            return {"error": "No videos were successfully downloaded"}

        # Verify all video files exist and have content
        for i, path in enumerate(video_paths):
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                logger.error(f"Video file {path} is missing or empty")
                return {"error": f"Video file {i+1} is missing or empty"}

        # Load video clips with error handling
        video_clips = []
        for i, path in enumerate(video_paths):
            try:
                logger.info(f"Loading video clip {i+1} from {path}")
                clip = VideoFileClip(path)
                # Basic validation of the clip
                if clip.duration <= 0 or clip.size[0] <= 0 or clip.size[1] <= 0:
                    logger.error(f"Invalid video dimensions or duration in {path}")
                    return {"error": f"Video {i+1} has invalid dimensions or duration"}
                video_clips.append(clip)
            except Exception as e:
                logger.error(f"Error loading video {i+1} from {path}: {str(e)}")
                return {"error": f"Error loading video {i+1}: {str(e)}"}

        # Concatenate videos with error handling
        try:
            if not video_clips:
                logger.error("No valid video clips to concatenate")
                return {"error": "No valid video clips to concatenate"}

            logger.info(f"Concatenating {len(video_clips)} video clips")
            final_clip = concatenate_videoclips(video_clips)

            # Validate the final clip
            if (
                final_clip.duration <= 0
                or final_clip.size[0] <= 0
                or final_clip.size[1] <= 0
            ):
                logger.error("Concatenated clip has invalid dimensions or duration")
                return {"error": "Failed to create valid concatenated video"}

            logger.info(
                f"Successfully created concatenated clip with duration {final_clip.duration}s"
            )
        except Exception as e:
            logger.error(f"Error concatenating video clips: {str(e)}")
            return {"error": f"Error concatenating video clips: {str(e)}"}

        # Process audio files
        audio_tracks = []

        # Background audio
        if background_audio_url:
            try:
                bg_audio_path = os.path.join(request_temp_dir, "background.mp3")
                logger.info(f"Downloading background audio from {background_audio_url}")
                if download_file(background_audio_url, bg_audio_path):
                    # Verify audio file
                    if (
                        not os.path.exists(bg_audio_path)
                        or os.path.getsize(bg_audio_path) == 0
                    ):
                        logger.error(f"Background audio file is missing or empty")
                        return {"error": "Background audio file is missing or empty"}

                    logger.info(f"Loading background audio from {bg_audio_path}")
                    bg_audio = AudioFileClip(bg_audio_path)

                    # Validate audio clip
                    if bg_audio.duration <= 0:
                        logger.error(
                            f"Invalid background audio duration: {bg_audio.duration}"
                        )
                        return {"error": "Background audio has invalid duration"}

                    logger.info(
                        f"Background audio duration: {bg_audio.duration}s, video duration: {final_clip.duration}s"
                    )

                    # Loop background audio if it's shorter than the final video
                    if bg_audio.duration < final_clip.duration:
                        logger.info(f"Looping background audio to match video duration")
                        bg_audio = bg_audio.loop(duration=final_clip.duration)
                    else:
                        # Trim background audio if it's longer than the final video
                        logger.info(
                            f"Trimming background audio to match video duration"
                        )
                        bg_audio = bg_audio.subclip(0, final_clip.duration)

                    # Set volume for background audio
                    logger.info(
                        f"Setting background audio volume to {background_volume}"
                    )
                    bg_audio = bg_audio.volumex(background_volume)
                    audio_tracks.append(bg_audio)
                else:
                    logger.error(
                        f"Failed to download background audio from {background_audio_url}"
                    )
                    return {"error": "Failed to download background audio"}
            except Exception as e:
                logger.error(f"Error processing background audio: {str(e)}")
                return {"error": f"Error processing background audio: {str(e)}"}

        # Narration audio
        have_narration = False
        narration_path = os.path.join(request_temp_dir, "narration.mp3")
        narration_ready = False

        # Handle narration input - could be a file object or a string URL
        if narration_file:
            # Check if narration_file is a string (URL) or file object
            if isinstance(narration_file, str):
                # It's a URL, treat it as narration_url
                logger.info(f"Treating narration_file as URL: {narration_file}")
                try:
                    # Check if it's a Google Drive URL
                    if (
                        "drive.google.com" in narration_file
                        or "docs.google.com" in narration_file
                    ):
                        download_success = download_drive_file(
                            narration_file, narration_path
                        )
                    else:
                        download_success = download_file(narration_file, narration_path)

                    narration_ready = download_success
                    if not download_success:
                        logger.error(
                            f"Failed to download narration file from {narration_file}"
                        )
                        logger.info("Continuing without narration audio")
                except Exception as e:
                    logger.error(f"Error downloading narration file: {str(e)}")
                    logger.info("Continuing without narration audio")
            else:
                # It's a file object
                try:
                    logger.info(f"Saving uploaded narration file to {narration_path}")
                    # Reset file cursor to beginning
                    await narration_file.seek(0)
                    # Save narration file
                    with open(narration_path, "wb") as buffer:
                        shutil.copyfileobj(narration_file.file, buffer)

                    narration_ready = True
                except Exception as e:
                    logger.error(f"Error saving uploaded narration file: {str(e)}")
                    logger.info("Continuing without narration audio")

        # Handle narration from separate URL parameter (for backward compatibility)
        elif narration_url:
            # Use the same URL handling code by treating narration_url as narration_file
            logger.info(
                f"Processing narration from narration_url parameter: {narration_url}"
            )
            try:
                # Check if it's a Google Drive URL
                if (
                    "drive.google.com" in narration_url
                    or "docs.google.com" in narration_url
                ):
                    download_success = download_drive_file(
                        narration_url, narration_path
                    )
                else:
                    download_success = download_file(narration_url, narration_path)

                narration_ready = download_success
                if not download_success:
                    logger.error(
                        f"Failed to download narration file from {narration_url}"
                    )
                    logger.info("Continuing without narration audio")
            except Exception as e:
                logger.error(f"Error downloading narration file: {str(e)}")
                logger.info("Continuing without narration audio")

        # Process narration file if it was successfully obtained
        if narration_ready:
            # Verify file exists and has content
            if os.path.exists(narration_path) and os.path.getsize(narration_path) > 0:
                try:
                    logger.info(f"Loading narration audio from {narration_path}")
                    narration_audio = AudioFileClip(narration_path)

                    # Trim narration if it's longer than the final video
                    if narration_audio.duration > final_clip.duration:
                        logger.info(
                            f"Trimming narration from {narration_audio.duration}s to {final_clip.duration}s"
                        )
                        narration_audio = narration_audio.subclip(
                            0, final_clip.duration
                        )

                    audio_tracks.append(narration_audio)
                    have_narration = True
                except Exception as inner_e:
                    logger.error(f"Error processing narration audio: {str(inner_e)}")
                    # Don't use the narration file if we can't load it
                    logger.warning(
                        "Unable to use the narration file, continuing without narration"
                    )
            else:
                logger.warning(
                    f"Narration file is missing or empty, skipping narration audio"
                )

            # Log the outcome for clarity
            if have_narration:
                logger.info("Successfully added narration audio")
            else:
                logger.info("Proceeding without narration audio")

        # Combine audio tracks with video
        if audio_tracks:
            final_audio = CompositeAudioClip(audio_tracks)
            final_clip = final_clip.set_audio(final_audio)

        # Export the final clip
        output_filename = f"merged_video_{request_id}.mp4"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        logger.info(f"Saving output file to: {output_path}")

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Check if output directory is writable
        if not os.access(os.path.dirname(output_path), os.W_OK):
            logger.error(
                f"Output directory is not writable: {os.path.dirname(output_path)}"
            )
            return {"error": "Output directory is not writable"}

        # Writing video file
        logger.info(
            f"Writing final video to {output_path} with codec libx264 and audio codec aac"
        )

        # Generate output file path
        temp_output_file_path = os.path.join(
            request_temp_dir, f"temp_merged_video_{request_id}.mp4"
        )
        output_file_path = os.path.join(OUTPUT_DIR, f"merged_video_{request_id}.mp4")
        logger.info(f"Writing merged video to {temp_output_file_path}")

        try:
            # Write the result with optimized parameters to prevent broken pipe errors
            logger.info(
                "Writing video with optimized parameters to prevent broken pipe errors"
            )
            # Ensure temp_audiofile is a string path, not an AudioFileClip object
            temp_audio_path = os.path.join(request_temp_dir, "temp_audio.m4a")
            final_clip.write_videofile(
                temp_output_file_path,
                codec="libx264",
                audio_codec="aac",
                temp_audiofile=temp_audio_path,
                remove_temp=True,
                threads=1,  # Reduce to 1 thread to avoid resource competition
                logger=None,
                ffmpeg_params=["-preset", "ultrafast", "-bufsize", "2000k"],
                verbose=False,
                write_logfile=True,  # Write logs to help with debugging
            )
            # Add subtitles if provided
            if subtitle_data and subtitle_data.get("subtitle_text"):
                logger.info(
                    f"Adding subtitle text: {subtitle_data.get('subtitle_text')}"
                )

                # Check audio duration with ffprobe and pad if needed (AWS requires min 0.5s)
                try:
                    # Get audio duration with ffprobe
                    import subprocess

                    duration_cmd = [
                        "ffprobe",
                        "-v",
                        "error",
                        "-show_entries",
                        "format=duration",
                        "-of",
                        "default=noprint_wrappers=1:nokey=1",
                        narration_path,
                    ]
                    duration_result = subprocess.run(
                        duration_cmd, capture_output=True, text=True, check=True
                    )
                    audio_duration = float(duration_result.stdout.strip())
                    logger.info(f"Audio duration: {audio_duration} seconds")

                    # If audio is shorter than 0.5 seconds, pad it
                    if audio_duration < 0.5:
                        logger.warning(
                            f"Audio too short: {audio_duration}s. Adding silence to meet 0.5s minimum."
                        )
                        padded_path = os.path.join(
                            request_temp_dir, "padded_narration.mp3"
                        )

                        # Create silence and concatenate
                        padding_needed = (
                            0.55 - audio_duration
                        )  # Add extra padding to be safe
                        silence_path = os.path.join(request_temp_dir, "silence.mp3")

                        # Create silence file
                        silence_cmd = [
                            "ffmpeg",
                            "-f",
                            "lavfi",
                            "-i",
                            "anullsrc=r=44100:cl=mono",
                            "-t",
                            str(padding_needed),
                            "-q:a",
                            "0",
                            "-c:a",
                            "libmp3lame",
                            silence_path,
                            "-y",
                        ]
                        subprocess.run(silence_cmd, check=True, capture_output=True)

                        # Create concat file
                        concat_file = os.path.join(request_temp_dir, "concat.txt")
                        with open(concat_file, "w") as f:
                            f.write(f"file '{narration_path}'\nfile '{silence_path}'")

                        # Concatenate files
                        concat_cmd = [
                            "ffmpeg",
                            "-f",
                            "concat",
                            "-safe",
                            "0",
                            "-i",
                            concat_file,
                            "-c",
                            "copy",
                            padded_path,
                            "-y",
                        ]
                        subprocess.run(concat_cmd, check=True, capture_output=True)

                        # Use padded file instead
                        narration_path = padded_path
                        logger.info(f"Using padded audio: {narration_path}")
                except Exception as e:
                    logger.warning(f"Error checking/padding audio: {str(e)}")
                    # Continue with original file if padding fails

                files = {"myfile": open(narration_path, "rb")}
                data = {
                    "params": json.dumps(
                        {
                            "template_id": "c793ad80c401438eb9c33b2ac2df058e",
                            "auth": {"key": "9b6f2d174f9240a58553008c2e047259"},
                        }
                    )
                }

                response = requests.post(
                    "https://api2.transloadit.com/assemblies", files=files, data=data
                )
                if response.status_code == 200:
                    print("API call successful:", response.json())

                import time

                assembly_ssl_url = response.json().get("assembly_ssl_url")
                if assembly_ssl_url:
                    while True:
                        poll_response = requests.get(assembly_ssl_url)
                        poll_data = poll_response.json()
                        if (
                            poll_data.get("ok") == "ASSEMBLY_COMPLETED"
                            and poll_response.status_code == 200
                        ):
                            print(
                                "The Assembly was successfully completed.",
                                poll_response,
                            )
                            # Extract the SRT file URL from the assembly result
                            transcribed_results = poll_data.get("results", {}).get(
                                "transcribed", []
                            )
                            srt_url = None
                            if transcribed_results and len(transcribed_results) > 0:
                                srt_url = transcribed_results[0].get("ssl_url")

                            # Download the SRT file if the URL is available
                            if srt_url:
                                srt_response = requests.get(srt_url)
                                if srt_response.status_code == 200:
                                    srt_path = os.path.join(
                                        request_temp_dir, "subtitle.srt"
                                    )
                                    with open(srt_path, "wb") as srt_file:
                                        srt_file.write(srt_response.content)
                                    print("Downloaded SRT file successfully:", srt_path)
                                else:
                                    print(
                                        "Failed to download SRT file, HTTP status:",
                                        srt_response.status_code,
                                    )
                            else:
                                print("No SRT URL found in the assembly result.")
                            break
                        else:
                            print(f"Polling for assembly completion...", poll_data)
                            time.sleep(10)  # Poll every 10 seconds
                else:
                    print("No assembly URL found in the response.")

                subtitle_file = os.path.join(request_temp_dir, "subtitle.srt")

                # FFmpeg command to add subtitles to center of video
                import subprocess
                import time

                ffmpeg_cmd = [
                    "ffmpeg",
                    "-i",
                    temp_output_file_path,
                    "-vf",
                    f"subtitles={subtitle_file}:force_style='FontName=Montserrat SemiBold,Fontsize=20,Alignment=10,PrimaryColour=&H00FFFFFF,Outline=0,OutlineColour=&H00000000,Shadow=1,Bold=1,MarginV=40'",
                    "-c:a",
                    "copy",
                    "-preset",
                    "ultrafast",
                    "-bufsize",
                    "2000k",
                    output_file_path,
                    "-y",
                ]

                logger.info(f"Running FFmpeg command: {' '.join(ffmpeg_cmd)}")

                # Add retry mechanism for FFmpeg operations
                max_retries = 3
                retry_delay = 2  # seconds

                for attempt in range(max_retries):
                    try:
                        logger.info(f"FFmpeg attempt {attempt+1}/{max_retries}")
                        result = subprocess.run(
                            ffmpeg_cmd, check=True, capture_output=True, text=True
                        )
                        logger.info("Successfully added subtitles to video")
                        break
                    except subprocess.CalledProcessError as e:
                        logger.error(f"FFmpeg error on attempt {attempt+1}: {str(e)}")
                        logger.error(f"FFmpeg stderr: {e.stderr}")

                        if "Broken pipe" in e.stderr or "pipe:" in e.stderr:
                            logger.warning(
                                "Detected broken pipe error, will retry with different parameters"
                            )
                            # Modify command to use different parameters on retry
                            ffmpeg_cmd.extend(["-max_muxing_queue_size", "9999"])

                        if attempt < max_retries - 1:
                            logger.info(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            logger.error("All FFmpeg retry attempts failed")
                            raise
            else:
                # If no subtitles, just copy the file to the output directory
                shutil.copy(temp_output_file_path, output_file_path)

            logger.info(f"Successfully wrote video file to: {output_path}")

            # Verify file was created
            if os.path.exists(output_path):
                logger.info(
                    f"Verified file exists: {output_path}, size: {os.path.getsize(output_path)} bytes"
                )

                # Create a permanent copy with a fixed name
                permanent_output_path = os.path.join(
                    OUTPUT_DIR, "final_merged_video.mp4"
                )
                logger.info(f"Creating permanent copy at: {permanent_output_path}")
                try:
                    shutil.copy2(output_path, permanent_output_path)
                    logger.info(f"Permanent copy created successfully")

                    # Verify permanent copy was created
                    if os.path.exists(permanent_output_path):
                        logger.info(
                            f"Verified permanent copy exists: {permanent_output_path}, size: {os.path.getsize(permanent_output_path)} bytes"
                        )
                    else:
                        logger.error(
                            f"Failed to create permanent copy at: {permanent_output_path}"
                        )
                except Exception as copy_error:
                    logger.error(f"Error creating permanent copy: {str(copy_error)}")
            else:
                logger.error(f"File was not created: {output_path}")
                return {"error": "Failed to create output file"}
        except Exception as e:
            logger.error(f"Error writing video file: {str(e)}")
            return {"error": f"Error writing video file: {str(e)}"}

        # Close all clips to release resources
        logger.info("Closing all video and audio clips to release resources")
        try:
            final_clip.close()
            for i, clip in enumerate(video_clips):
                try:
                    clip.close()
                except Exception as e:
                    logger.warning(f"Error closing video clip {i}: {str(e)}")
            for i, track in enumerate(audio_tracks):
                try:
                    track.close()
                except Exception as e:
                    logger.warning(f"Error closing audio track {i}: {str(e)}")
        except Exception as e:
            logger.warning(f"Error during resource cleanup: {str(e)}")
            # Continue execution even if cleanup fails

        # Upload to Google Drive if requested
        drive_upload_result = None
        if upload_to_drive_bool:
            try:
                logger.info(f"Uploading video to Google Drive: {output_path}")
                drive_upload_result = upload_video(output_path)
                logger.info(
                    f"Successfully uploaded to Google Drive: {drive_upload_result}"
                )

                # We'll clean up files after streaming, not immediately after upload
                logger.info(
                    "Upload successful, but will clean up files after streaming"
                )

                # We'll verify the file still exists before streaming
                if not os.path.exists(output_path):
                    logger.error(
                        f"Output file no longer exists after upload: {output_path}"
                    )
                    # At this point, permanent_output_path might not be defined yet
                    # We'll check for any permanent copy in the output directory
                    permanent_files = [
                        f
                        for f in os.listdir(OUTPUT_DIR)
                        if f.startswith("permanent_copy_")
                        or f == "final_merged_video.mp4"
                    ]
                    if permanent_files:
                        recovery_file = os.path.join(OUTPUT_DIR, permanent_files[0])
                        logger.info(
                            f"Restoring output file from permanent copy: {recovery_file}"
                        )
                        shutil.copy2(recovery_file, output_path)

                # Verify files exist
                temp_files_remaining = (
                    os.listdir(TEMP_DIR) if os.path.exists(TEMP_DIR) else []
                )
                output_files_remaining = (
                    os.listdir(OUTPUT_DIR) if os.path.exists(OUTPUT_DIR) else []
                )

                logger.info(
                    f"Cleanup verification - Temp files remaining: {len(temp_files_remaining)}, Output files remaining: {len(output_files_remaining)}"
                )

                # If files still remain, try one more time with forced cleanup
                if temp_files_remaining or output_files_remaining:
                    logger.warning(
                        "Some files remained after cleanup, attempting forced cleanup"
                    )
                    try:
                        # Force remove all files with more aggressive approach
                        for dir_path in [TEMP_DIR, OUTPUT_DIR]:
                            if os.path.exists(dir_path):
                                for item in os.listdir(dir_path):
                                    item_path = os.path.join(dir_path, item)
                                    try:
                                        if os.path.isdir(item_path):
                                            shutil.rmtree(item_path, ignore_errors=True)
                                        else:
                                            os.remove(item_path)
                                    except Exception as e:
                                        logger.error(
                                            f"Failed to remove {item_path}: {str(e)}"
                                        )
                    except Exception as cleanup_error:
                        logger.error(
                            f"Error during forced cleanup: {str(cleanup_error)}"
                        )

                    # Final verification
                    temp_files_remaining = (
                        os.listdir(TEMP_DIR) if os.path.exists(TEMP_DIR) else []
                    )
                    output_files_remaining = (
                        os.listdir(OUTPUT_DIR) if os.path.exists(OUTPUT_DIR) else []
                    )
                    logger.info(
                        f"Final cleanup verification - Temp files remaining: {len(temp_files_remaining)}, Output files remaining: {len(output_files_remaining)}"
                    )

                # Re-create empty directories to ensure they exist for future requests
                os.makedirs(TEMP_DIR, exist_ok=True)
                os.makedirs(OUTPUT_DIR, exist_ok=True)
            except Exception as e:
                logger.error(f"Error uploading to Google Drive: {str(e)}")
                drive_upload_result = {"error": str(e)}

            if upload_to_drive_bool:
                logger.info(
                    "Upload was requested, cleaning up all temp and output files"
                )
                background_tasks.add_task(cleanup_all_temp_output)
            else:
                logger.info(f"Scheduling cleanup of specific files")
                # Add permanent_output_path to cleanup list if it exists
                files_to_clean = [request_temp_dir, output_path]
                if os.path.exists(permanent_output_path):
                    files_to_clean.append(permanent_output_path)

    except Exception as e:
        logger.error(f"Error in merge_videos: {str(e)}")

        # Create a list of files to clean up
        files_to_cleanup = [request_temp_dir]

        # Add output files if they exist
        if "output_path" in locals() and os.path.exists(output_path):
            files_to_cleanup.append(output_path)

        if "permanent_output_path" in locals() and os.path.exists(
            permanent_output_path
        ):
            files_to_cleanup.append(permanent_output_path)

        # Clean up all temporary files
        cleanup_files(files_to_cleanup)

        # Return a more user-friendly error message
        if "narration.mp3" in str(e):
            return {
                "error": "There was an issue with the narration file. Please make sure it's a valid audio file."
            }
        else:
            return {"error": str(e)}
    return {"message": drive_upload_result}


@app.get("/")
async def root():
    logger.info("Root endpoint accessed")

    # Check if output directory exists and is writable
    output_dir_exists = os.path.exists(OUTPUT_DIR)
    output_dir_writable = os.access(OUTPUT_DIR, os.W_OK) if output_dir_exists else False

    # Check if temp directory exists and is writable
    temp_dir_exists = os.path.exists(TEMP_DIR)
    temp_dir_writable = os.access(TEMP_DIR, os.W_OK) if temp_dir_exists else False

    return {
        "message": "Video Merger API is running. Use /merge endpoint to merge videos.",
        "status": "ok",
        "directories": {
            "output_dir": {
                "path": OUTPUT_DIR,
                "exists": output_dir_exists,
                "writable": output_dir_writable,
            },
            "temp_dir": {
                "path": TEMP_DIR,
                "exists": temp_dir_exists,
                "writable": temp_dir_writable,
            },
        },
    }


@app.get("/check-directories")
async def check_directories():
    """Check and create output and temp directories"""
    logger.info("Check directories endpoint accessed")

    # Ensure directories exist
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if directories are writable
    temp_dir_writable = os.access(TEMP_DIR, os.W_OK)
    output_dir_writable = os.access(OUTPUT_DIR, os.W_OK)

    # Create a test file in the output directory to verify write permissions
    test_file_path = os.path.join(OUTPUT_DIR, "test_write.txt")
    test_file_success = False
    try:
        with open(test_file_path, "w") as f:
            f.write("Test write access")
        test_file_success = True
        # Clean up test file
        os.remove(test_file_path)
    except Exception as e:
        logger.error(f"Failed to write test file: {str(e)}")

    return {
        "status": "ok",
        "directories": {
            "temp_dir": {
                "path": TEMP_DIR,
                "exists": os.path.exists(TEMP_DIR),
                "writable": temp_dir_writable,
            },
            "output_dir": {
                "path": OUTPUT_DIR,
                "exists": os.path.exists(OUTPUT_DIR),
                "writable": output_dir_writable,
                "test_write_success": test_file_success,
            },
        },
    }


@app.get("/list-videos")
async def list_videos():
    """Information about video streaming"""
    logger.info("List videos endpoint accessed")

    return {
        "message": "Videos are not being saved to the server. They are streamed directly to the client upon creation.",
        "videos": [],
    }


@app.get("/verify-cleanup")
async def verify_cleanup(run_cleanup: bool = False):
    """Verify that cleanup process is working correctly"""
    logger.info(f"Verify cleanup endpoint accessed with run_cleanup={run_cleanup}")

    # Check temp directory
    temp_files = []
    if os.path.exists(TEMP_DIR) and os.path.isdir(TEMP_DIR):
        temp_files = os.listdir(TEMP_DIR)

    # Check output directory
    output_files = []
    if os.path.exists(OUTPUT_DIR) and os.path.isdir(OUTPUT_DIR):
        output_files = os.listdir(OUTPUT_DIR)

    # Run cleanup if requested via query parameter
    cleanup_requested = run_cleanup
    if cleanup_requested:
        logger.info("Running cleanup as requested")
        cleanup_all_temp_output()

        # Check directories again after cleanup
        temp_files_after = []
        if os.path.exists(TEMP_DIR) and os.path.isdir(TEMP_DIR):
            temp_files_after = os.listdir(TEMP_DIR)

        output_files_after = []
        if os.path.exists(OUTPUT_DIR) and os.path.isdir(OUTPUT_DIR):
            output_files_after = os.listdir(OUTPUT_DIR)

        return {
            "status": "cleanup_performed",
            "before_cleanup": {
                "temp_directory": {
                    "path": TEMP_DIR,
                    "file_count": len(temp_files),
                    "files": temp_files,
                },
                "output_directory": {
                    "path": OUTPUT_DIR,
                    "file_count": len(output_files),
                    "files": output_files,
                },
            },
            "after_cleanup": {
                "temp_directory": {
                    "path": TEMP_DIR,
                    "file_count": len(temp_files_after),
                    "files": temp_files_after,
                },
                "output_directory": {
                    "path": OUTPUT_DIR,
                    "file_count": len(output_files_after),
                    "files": output_files_after,
                },
            },
        }

    # Just return current state without cleanup
    return {
        "status": "current_state",
        "temp_directory": {
            "path": TEMP_DIR,
            "file_count": len(temp_files),
            "files": temp_files,
        },
        "output_directory": {
            "path": OUTPUT_DIR,
            "file_count": len(output_files),
            "files": output_files,
        },
        "message": "To run cleanup, add ?run_cleanup=true to the URL",
        "example": "/verify-cleanup?run_cleanup=true",
    }


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting server on port 8001")
    uvicorn.run(app, host="0.0.0.0", port=8001)
