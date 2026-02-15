# Contributing guidelines

This document provides guidelines for contributing to the DSIS Python Client project.

## Requesting changes

[Create a new issue](https://github.com/equinor/dsis-python-client/issues/new/choose).

## Making changes

1. Create a new branch. For external contributors, create a fork.
1. Make your changes.
1. Commit your changes.

    Follow the [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0/) for semantic commit messages.

1. Create a pull request to merge your changes into branch `main`.

## Adding example notebooks

Example notebooks are rendered as static pages on the docs site using [mkdocs-jupyter](https://github.com/danielfrg/mkdocs-jupyter). To add a new one:

1. Place your `.ipynb` file in the `docs/notebooks/` directory.
2. Run the notebook locally and save it with cell outputs included — these will be displayed on the docs site as-is (notebooks are not executed during the docs build).
3. Add a nav entry in `mkdocs.yml` under the `Examples` section:

    ```yaml
    - Examples:
        - Your Example Title: notebooks/your_notebook.ipynb
    ```

4. Avoid committing credentials or secrets in cell outputs!
