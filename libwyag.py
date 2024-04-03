import argparse
import collections
import configparser
from datetime import datetime
import grp, pwd
from fnmatch import fnmatch
import hashlib
from math import ceil
import re
import sys
import zlib
import os

argparser = argparse.ArgumentParser(description="The stupidest content tracker")

argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsubparsers.required = True

def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)
    match args.command:
        case "init"     : cmd_init(args)
        case _          : print("Bad command.")

class GitRepository(object):
    """A git repository"""

    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False) -> None:
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not force or os.path.isdir(self.gitdir):
            raise Exception(f"Not a Git repository {path}")
        
        self.conf = configparser.ConfigParser()
        cf = repo_file(self, "config")

        if cf and os.path.exists(cf):
            self.conf.read([cf])
        elif not force:
            raise Exception("Configuration file missing")
        
        if not force:
            vers = int(self.conf.get("core", "repositoryformatversion"))

            if vers != 0:
                raise Exception(f"Unsupported repositoryformatversion {vers}")
            
def repo_path(repo, *path):
    """Compute path under repo's gitdir"""
    return os.path.join(repo.gitdir, *path)

def repo_file(repo, *path, mkdir=False):
    """Same as repo_path, but create dirname(*path) if absent.
       For example, repo_file(r, \"refs\", \"remotes\", \"origin\", \"HEAD\") will create
       .git/refs/remotes/origin."""
    
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)
    
def repo_dir(repo, *path, mkdir=False):
    """Same as repo_path, but mkdir *path if absent"""

    path = repo_path(repo, *path)

    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception(f"Not a directory {path}")
        
    if mkdir:
        os.makedirs(path)
        return path
    else:
        return None
        
def repo_create(path):
    """Create a new repository at path."""

    repo = GitRepository(path, True)

    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception(f"{path} is not a directory")
        if os.path.exists(repo.gitdir) and os.listdir(repo.gitdir):
            raise Exception(f"{path}")
    else:
        os.makedirs(repo.worktree)

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)
    assert repo_dir(repo, "refs", "tags", mkdir=True)
    assert repo_dir(repo, "refs", "heads", mkdir=True)

    with open(repo_file(repo, "description"), "w") as f:
        f.write("Unnamed repository; edit this file 'description' to name the repository.\n")

    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/master\n")

    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)

    return repo
        
def repo_default_config():
    ret = configparser.ConfigParser()

    ret.add_section("core")
    ret.set("core", "repositoryformatversion", "0")
    ret.set("core", "filemode", "false")
    ret.set("core", "bare", "false")

    return ret

argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository")

argsp.add_argument("path",
                   metavar="directory",
                   nargs="?",
                   default=".",
                   help="Where to create the repository")

def cmd_init(args):
    repo_create(args.path)

def repo_find(path=".", required=True):
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)
    
    parent = os.path.realpath(os.path.join(".."))

    if parent == path:
        if required:
            return Exception("No git directory found")
        else:
            return None
    
    return repo_find(parent, required)

class GitObject(object):

    def __init__(self, data=None) -> None:
        if data:
            self.deserialise(data)
        else:
            self.init()

    def serialise(self, repo):
        raise Exception("Unimplemented")
    
    def deserialise(self, data):
        raise Exception("Unimplemented")
    
    def init(self):
        pass

def object_read(repo, sha):
    """Read object sha from Git repository repo.  Return a
    GitObject whose exact type depends on the object."""

    path = repo_file(repo, "objects", sha[:2], sha[2:])

    if not os.path.isfile(path):
        return None
    
    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())

        # read object type
        x = raw.find(b' ')
        fmt = raw[:x]

        # read and validate object size
        y = raw.find(b'\x00', x)
        size = int(raw[x:y].decode('ascii'))

        if size != len(raw) - y - 1:
            raise Exception(f"Malformed objext {sha}: bad length")
        
        match fmt:
            case b'commit'  : c=GitCommit
            case b'tree'    : c=GitTree
            case b'tag'     : c=GitTag
            case b'blob'    : c=GitBlob
            case _:
                raise Exception(f"Unknown type {fmt.decode('ascii')} for object {sha}")
            
        return c(raw[y + 1:])

def object_write(obj, repo=None):
    data = obj.serialise()

    # header
    result = obj.fmt + b' ' + str(len(data)).encode() + b'\x00' + data

    # compute hash
    sha = hashlib.sha1(result).hexdigest()

    if repo:
        path = repo_file(repo, "objects", sha[:2], sha[2:], mkdir=True)

        if not os.path.exists(path):
            with open(path, 'wb') as f:
                f.write(zlib.compress(result))

    return sha

class GitBlob(GitObject):
    fmt = b'blob'

    def serialise(self, repo):
        return self.blobdata
    
    def deserialise(self, data):
        self.blobdata = data