# download-manager
Parallelize HTTP downloads.

This is a simple set of scripts to download large files (dozens of GBs) over HTTP. It uses many parallel connections and handles failures by keeping state in a file on disk.

Usage:

```
python download-manager.py <URL>
```

The python script uses unix shell scripts to actually perform downloads, so on Windows you'll have to run this in the WSL.
