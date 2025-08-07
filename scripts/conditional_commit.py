# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "srsly>=2.4.8",
#   "GitPython>=3.1.41"
# ]
# ///

"""
Conditional commit script for the Criterion Closet data pipeline.

This script intelligently commits changes based on the following rules:
- Always commit changes to enriched_playlist.json, playlist_with_subtitles.json, and report.md
- Only commit playlist.json if the number of unique video IDs has changed
"""

import json
import sys
from pathlib import Path
from typing import Set

import git
import srsly


def get_unique_video_ids(playlist_path: Path) -> Set[str]:
    """Extract unique video IDs from a playlist JSON file.
    
    Args:
        playlist_path: Path to the playlist JSON file
        
    Returns:
        Set of unique video IDs
    """
    if not playlist_path.exists():
        return set()
    
    try:
        with open(playlist_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract video IDs from the playlist structure
        video_ids = set()
        for entry in data:
            if 'id' in entry:
                video_ids.add(entry['id'])
            elif 'video_id' in entry:
                video_ids.add(entry['video_id'])
        
        return video_ids
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"Error reading playlist file {playlist_path}: {e}")
        return set()


def has_meaningful_playlist_change(repo: git.Repo, playlist_path: Path) -> bool:
    """Check if playlist.json has meaningful changes (different number of videos).
    
    Args:
        repo: GitPython repository object
        playlist_path: Path to the playlist JSON file
        
    Returns:
        True if the number of videos has changed, False otherwise
    """
    try:
        # Get the current video IDs
        current_ids = get_unique_video_ids(playlist_path)
        
        # Get the video IDs from the last commit
        try:
            # Get the file content from HEAD
            head_content = repo.git.show(f'HEAD:{playlist_path}')
            head_data = json.loads(head_content)
            
            head_ids = set()
            for entry in head_data:
                if 'id' in entry:
                    head_ids.add(entry['id'])
                elif 'video_id' in entry:
                    head_ids.add(entry['video_id'])
                    
        except (git.GitCommandError, json.JSONDecodeError, KeyError, TypeError):
            # If we can't get the HEAD version, assume it's a meaningful change
            return True
        
        # Compare the number of unique video IDs
        return len(current_ids) != len(head_ids)
        
    except Exception as e:
        print(f"Error checking playlist changes: {e}")
        return True  # Default to committing on error


def main():
    """Main entry point for the conditional commit script."""
    try:
        # Initialize git repository
        repo = git.Repo('.')
        
        # Check if there are any changes to commit
        if not repo.is_dirty() and not repo.untracked_files:
            print("No changes to commit")
            return
        
        # Get list of changed files
        changed_files = [item.a_path for item in repo.index.diff(None)]
        changed_files.extend([item.a_path for item in repo.index.diff("HEAD")])
        changed_files.extend(repo.untracked_files)
        changed_files = list(set(changed_files))  # Remove duplicates
        
        print(f"Changed files: {changed_files}")
        
        # Define file paths
        playlist_path = Path('data/playlist.json')
        other_data_files = [
            'data/enriched_playlist.json',
            'data/playlist_with_subtitles.json',
            'report.md'
        ]
        
        # Check what files have changed
        playlist_changed = str(playlist_path) in changed_files
        other_files_changed = any(f in changed_files for f in other_data_files)
        
        files_to_commit = []
        
        # Always add non-playlist data files if they've changed
        for file_path in other_data_files:
            if file_path in changed_files:
                files_to_commit.append(file_path)
        
        # Handle playlist.json conditionally
        if playlist_changed:
            if has_meaningful_playlist_change(repo, playlist_path):
                print("Playlist has meaningful changes (video count changed)")
                files_to_commit.append(str(playlist_path))
            else:
                print("Playlist changes are not meaningful (same video count)")
                # Reset the playlist file to discard non-meaningful changes
                repo.git.checkout('HEAD', str(playlist_path))
        
        # Commit files if there are any to commit
        if files_to_commit:
            print(f"Committing files: {files_to_commit}")
            
            # Configure git user
            repo.git.config('--global', 'user.name', 'github-actions[bot]')
            repo.git.config('--global', 'user.email', 'github-actions[bot]@users.noreply.github.com')
            
            # Add files to staging area
            repo.index.add(files_to_commit)
            
            # Create commit message
            if len(files_to_commit) == 1:
                commit_message = f"Update {files_to_commit[0]}"
            else:
                commit_message = "Update data pipeline artifacts"
            
            # Commit and push
            repo.index.commit(commit_message)
            origin = repo.remote('origin')
            origin.push()
            
            print(f"Successfully committed and pushed: {commit_message}")
        else:
            print("No meaningful changes to commit")
            
    except Exception as e:
        print(f"Error in conditional commit: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
