#!/usr/bin/env python
# coding: utf-8

import os, pathlib

def init_symlinks():
    """Recreate symlinks of this project and all subprojects."""
    print('Initializing symlinks for the project "pubdata".')
    root_dir = _dir_up()
    print(f'VERIFY! Project root directory: "{root_dir}"')
    
    _recreate_dir_symlink('nbs/pubdata', '../pubdata', root_dir)
    _recreate_dir_symlink('pubdata/reseng', '../submodules/reseng/reseng', root_dir)
    
    # test
    import pubdata
    import pubdata.reseng

def _dir_up():
    """Return dir path two levels above current notebook or script."""
    try:
        caller_dir = pathlib.Path(__file__).parent.resolve()
    except Exception as e:
        if str(e) != "name '__file__' is not defined": raise
        caller_dir = pathlib.Path.cwd()
    return caller_dir.parent

def _recreate_dir_symlink(link, targ, root):
    """Remove and create new symlink from `link` to `targ`.
    `link` must be relative to `root`.
    `targ must be relative to directory containing `link`.
    """
    link = (root / link).absolute()
    assert (link.parent / targ).is_dir()
    link.unlink(missing_ok=True)
    link.symlink_to(pathlib.Path(targ), target_is_directory=True)
    link_res = link.resolve()
    assert link_res.is_dir()
    print(f'symlink: "{link.relative_to(root)}" -> "{link_res.relative_to(root)}"')

