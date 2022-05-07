
- English
- [中文](README_zh.MD)

# Smart Backup

### Feture

- Support *Full Backup* / *Incremental Backup* / *Differential Backup*
- Support **timed backup**
- Support **auto clean** outdated backups

### Notes

- The backup created with `!!smb make(full)` is **not going to** *auto clean*
- **Don't** change/remove files in `./smt_backup`, if you really need, please remove the **whole folder**
- If you remove `./smt_backup` when *the plugin is running*, that probably will cause some *unknown error*
