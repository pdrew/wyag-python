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
        case "init"         : cmd_init(args)
        case "cat-file"     : cmd_cat_file(args)
        case "hash-object"  : cmd_hash_object(args)
        case "log"          : cmd_log(args)
        case _              : print("Bad command.")

class GitRepository(object):
    """A git repository"""

    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False) -> None:
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not (force or os.path.isdir(self.gitdir)):
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

    def serialise(self, repo=None):
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
    data = obj.serialise(repo)

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

    def serialise(self, repo=None):
        return self.blobdata
    
    def deserialise(self, data):
        self.blobdata = data

argsp = argsubparsers.add_parser("cat-file", help="Provide content of repository objects")

argsp.add_argument("type",
                   metavar="type",
                   choices=["blob", "commit", "tag", "tree"],
                   help="Specify the type")

argsp.add_argument("object",
                   metavar="object",
                   help="The object to display")

def cmd_cat_file(args):
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())

def cat_file(repo, obj, fmt=None):
    obj = object_read(repo, object_find(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(obj.serialise())

def object_find(repo, name, fmt=None, follow=True):
    return name

argsp = argsubparsers.add_parser("hash-object", help="Compute object ID and optionally creates a blob from a file")

argsp.add_argument("-t",
                   metavar="type",
                   dest="type",
                   choices=["blob", "commit", "tag", "tree"],
                   help="Specify the type")

argsp.add_argument("-w",
                   dest="write",
                   action="store_true",
                   help="Actually write the object into the database")

argsp.add_argument("path",
                   help="Read object from <file>")

def cmd_hash_object(args):
    if args.write:
        repo = repo_find()
    else:
        repo = None

    with open(args.path, "rb") as fd:
        sha = object_hash(fd, args.type.encode(), repo)
        print(sha)

def object_hash(fd, fmt, repo=None):
    """Hash object, writing it to repo if provided"""

    data = fd.read()

    match fmt:
        case b'commit'  : obj=GitCommit(data)
        case b'tree'    : obj=GitTree(data)
        case b'tag'     : obj=GitTag(data)
        case b'blob'    : obj=GitBlob(data)
        case _          : raise Exception(f"Unknown type {fmt.decode()}")

    return object_write(obj, repo)

def kvlm_parse(raw, start=0, dct=None):
    """Key-Value List with Message parser"""

    if not dct:
        dct = collections.OrderedDict()

    space = raw.find(b' ', start)
    newline = raw.find(b'\n', start)

    # If space appears before newline, we have a keyword.  Otherwise,
    # it's the final message, which we just read to the end of the file.

    # Base case
    # =========
    # If newline appears first (or there's no space at all, in which
    # case find returns -1), we assume a blank line.  A blank line
    # means the remainder of the data is the message.  We store it in
    # the dictionary, with None as the key, and return.
    if space < 0 or newline < space:
        assert newline == start
        dct[None] = raw[start + 1:]
        return dct
    
    # Recursive case
    # ==============
    # we read a key-value pair and recurse for the next.
    key = raw[start:space]

    # Find the end of the value.  Continuation lines begin with a
    # space, so we loop until we find a "\n" not followed by a space.
    end = start
    while True:
        end = raw.find(b'\n', end + 1)
        if raw[end + 1] != ord(' '): 
            break
    
    # Grab the value
    # Also, drop the leading space on continuation lines
    value = raw[space + 1: end].replace(b'\n ', b'\n')

    if key in dct:
        if type(dct[key]) == list:
            dct[key].append(value)
        else:
            dct[key] = [dct[key], value]
    else:
        dct[key] = value

    return kvlm_parse(raw, start=end + 1, dct=dct)
    
def kvlm_serialise(kvlm):
    ret = b''

    for key in kvlm.keys():
        if not key:
            continue
        value = kvlm[key]
        if type(value) != list:
            value = [value]

        for v in value:
            ret += key + b' ' + v.replace(b'\n', b'\n ') + b'\n'

    ret += b'\n' + kvlm[None] + b'\n'

    return ret

class GitCommit(GitObject):
    fmt = b'commit'

    def deserialise(self, data):
        self.kvlm = kvlm_parse(data)

    def serialise(self, repo=None):
        return kvlm_serialise(self.kvlm)
    
    def init(self):
        self.kvlm = dict()

argsp = argsubparsers.add_parser("log", help="Display history of a given commit")

argsp.add_argument("commit",
                   default="HEAD",
                   nargs="?",
                   help="Commit to start at")

def cmd_log(args):
    repo = repo_find()

    print("digraph wyaglog{")
    print("  node[shape=rect]")
    log_graphviz(repo, object_find(repo, args.commit), set())
    print("}")

def log_graphviz(repo, sha, seen):
    if sha in seen:
        return
    
    seen.add(sha)

    commit = object_read(repo, sha)
    short_hash = sha[0:8]
    message = commit.kvlm[None].decode("utf8").strip()
    message = message.replace("\\", "\\\\")
    message = message.replace("\"", "\\\"")

    # Keep only the first line
    if "\n" in message:
        message = message[:message.index("\n")]

    print(f"  c_{sha} [label=\"{short_hash}: {message}\"]")
    assert commit.fmt == b'commit'

    if not b'parent' in commit.kvlm.keys():
        return
    
    parents = commit.kvlm[b'parent']

    if type(parents) != list:
        parents = [parents]

    for parent in parents:
        parent = parent.decode("ascii")
        print(f"  c_{sha} -> c_{parent};")
        log_graphviz(repo, parent, seen)
