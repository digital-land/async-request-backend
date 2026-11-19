"""
Deletes a request's working files without touching anything outside them.

Request ids come from task messages, so an id such as ".." or "/", or a
symlinked request directory, must not be able to point a recursive delete
somewhere else.
"""

import os

from application.configurations.config import Directories
from application.logging.logger import get_logger

logger = get_logger(__name__)


def _request_resource_dir(request_id):
    """
    Return the request's resource directory, or None if it is unsafe to delete.

    Only a direct child of the resource directory that is not a symlink is
    returned.
    """
    resource_parent = os.path.realpath(
        os.path.join(Directories.COLLECTION_DIR, "resource")
    )
    candidate = os.path.join(resource_parent, str(request_id))
    if os.path.islink(candidate):
        logger.error(f"Refusing to clean up {candidate!r}: it is a symlink")
        return None
    resource_dir = os.path.realpath(candidate)
    if os.path.dirname(resource_dir) != resource_parent:
        logger.error(
            f"Refusing to clean up {resource_dir!r}: outside {resource_parent!r}"
        )
        return None
    return resource_dir


def _remove_tree_without_following_links(path):
    # os.walk does not follow symlinks; symlinked dirs are listed in dirs and
    # removed as links, never traversed.
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            child = os.path.join(root, name)
            if os.path.islink(child):
                os.remove(child)
            else:
                os.rmdir(child)
    os.rmdir(path)


def clean_up_request_files(request_id):
    """
    Remove the resource directory Collector created for a request.

    Needed when a task fails, as the workflow will not clean up after itself.
    """
    resource_dir = _request_resource_dir(request_id)
    if resource_dir is None:
        return
    try:
        if os.path.exists(resource_dir):
            _remove_tree_without_following_links(resource_dir)

            # Clean up parent directories if empty
            resource_parent_dir = os.path.dirname(resource_dir)
            if os.path.exists(resource_parent_dir) and not os.listdir(
                resource_parent_dir
            ):
                os.rmdir(resource_parent_dir)
                collection_dir = os.path.dirname(resource_parent_dir)
                if os.path.exists(collection_dir) and not os.listdir(collection_dir):
                    os.rmdir(collection_dir)

    except Exception as e:
        logger.error(f"Failed to clean up resource directory {resource_dir}: {e}")


def _is_direct_child(path, parent):
    return os.path.dirname(os.path.realpath(path)) == os.path.realpath(parent)


def _clean_up_directory(request_id, directory):
    dir_path = os.path.join(directory, str(request_id))
    # Only ever delete a direct child of the given directory, so ids such as
    # ".." or "/" cannot point the delete at a parent directory, and never
    # follow a symlinked request directory.
    if os.path.islink(dir_path) or not _is_direct_child(dir_path, directory):
        logger.error(
            f"Refusing to clean up {dir_path!r}: not a directory inside {directory!r}"
        )
        return
    if os.path.exists(dir_path):
        for file in os.listdir(dir_path):
            file_path = os.path.join(dir_path, file)
            # Remove symlinks as links; never recurse into their targets.
            if os.path.islink(file_path) or os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                clean_up(request_id, file_path)
        # Check if the directory is empty after removing files
        if not os.listdir(dir_path):
            os.rmdir(dir_path)
    if os.path.exists(directory) and not os.listdir(directory):
        os.rmdir(directory)


def clean_up(request_id, *directories):
    """Remove <directory>/<request_id> for each directory, then empty parents."""
    try:
        for directory in directories:
            _clean_up_directory(request_id, directory)
    except Exception as e:
        logger.error(
            f"An error occurred during cleanup of {directory}: {e}", exc_info=True
        )
