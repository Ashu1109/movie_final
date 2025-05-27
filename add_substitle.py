import os
import subprocess
import json


def add_subtitle_to_video(video_path, request_temp_dir, audio_path):
    """
    Add subtitle to a video using FFmpeg.

    :param video_path: Path to the input video file
    :param request_temp_dir: Directory to store temporary files
    :param audio_path: Path to the audio file to be transcribed
    :return: Path to the output video with subtitles
    """

    # Verify that the video file exists
    if not os.path.exists(video_path):
        print(f"Error: Video file '{video_path}' not found.")
        return None

    # Ensure the temp directory exists
    if not os.path.exists(request_temp_dir):
        os.makedirs(request_temp_dir)

    import requests

    files = {"myfile": open(audio_path, "rb")}
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
                print("The Assembly was successfully completed.", poll_response)
                # Extract the SRT file URL from the assembly result
                transcribed_results = poll_data.get("results", {}).get("transcribed", [])
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
                print("Polling for assembly completion...")
                time.sleep(5)  # Poll every 5 seconds
    else:
        print("No assembly URL found in the response.")

    # subtitle_file = os.path.join(request_temp_dir, "subtitle.srt")

    # # Create SRT file with subtitles, breaking the subtitle_text into parts
    # if 'subtitle_text' in merge_data and merge_data['subtitle_text']:
    #     # Split the subtitle_text by periods to create separate sentences
    #     raw_subtitles = merge_data['subtitle_text'].split('.')
    #     # Clean up the sentences and remove empty ones
    #     subtitles = [subtitle.strip() for subtitle in raw_subtitles if subtitle.strip()]
    #     # Add periods back to the end of each subtitle if not already present
    #     subtitles = [subtitle + '.' if not subtitle.endswith('.') else subtitle for subtitle in subtitles]
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

    # Create a default subtitle file if no SRT was downloaded
    subtitle_file = os.path.join(request_temp_dir, "subtitle.srt")
    
    # If we didn't download an SRT file, create a default one
    if not os.path.exists(subtitle_file):
        # Default subtitles
        subtitles = [
            "Embark on a journey of timeless wisdom.",
            "From ancient Greece to modern life.",
            "In the shadow of Titans, we discover eternal truths etched in marble.",
            "Through ancient eyes, we see the present anew, a canvas of choices and change.",
            "The gods whisper through marble, guiding us toward unyielding resolve amid chaos.",
            "In contemplation, find the courage to act; in action, find the peace of mind.",
            "These lessons endure as marble, teaching us strength, patience, and enduring wisdom.",
        ]
        
        with open(subtitle_file, "w") as f:
            for i, subtitle in enumerate(subtitles):
                start_time = f"00:00:{i*5:02},000"
                end_time = f"00:00:{(i+1)*5:02},000"
                f.write(f"{i+1}\n{start_time} --> {end_time}\n{subtitle}\n\n")

    # Add subtitle to video using FFmpeg
    output_video_with_subtitle = os.path.join(
        request_temp_dir, "video_with_subtitle.mp4"
    )
    ffmpeg_cmd = [
        "ffmpeg",
        "-i",
        video_path,
        "-vf",
        f"subtitles={subtitle_file}:force_style='Fontsize=24,Alignment=10,PrimaryColour=&HFFFFFF,Outline=1,Shadow=0,MarginV=0,MarginL=0,MarginR=0,Bold=1'",
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


# Ensure temp directory exists
if not os.path.exists("temp"):
    os.makedirs("temp")

add_subtitle_to_video(
    "merged_video_84db5dcb-5948-45ea-b356-181246ff07e3 (1).mp4", "temp", "song.mp3"
)
