import mkdocs_gen_files

changelog_file = open("CHANGELOG.md")
changelog_content = changelog_file.read()

with mkdocs_gen_files.open("changelog/index.md", "w") as f:
    print(changelog_content, file=f)
