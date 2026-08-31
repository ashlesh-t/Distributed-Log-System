# Publishing Prerequisites & Steps

Before announcing the Distributed Log System (DLS) to the public or officially launching version 1.0, the following steps must be completed to ensure a smooth, secure, and professional user experience.

---

## Phase 1: Security & Codebase Cleanup
1. **Audit Git History:** Ensure no hardcoded passwords, API keys, or `.env` files with real credentials exist in the Git history. Use tools like `git-filter-repo` or BFG Repo-Cleaner if necessary.
2. **Resolve "Installation Issues":** The README notes existing installation issues. These must be debugged and fixed. A user's first experience must be flawless.
3. **Remove Hardcoded Values:** Ensure all hardcoded network names (e.g., `kafka:9092`, `http://elasticsearch:9200`) in the Python and Go code can be overridden seamlessly via environment variables.

## Phase 2: Automated CI/CD (GitHub Actions)
Create a `.github/workflows/` directory with the following automated pipelines:
1. **Linting & Formatting:** 
   * Python: Run `pylint`, `ruff`, and `black`.
   * Go: Run `golangci-lint` and `gofmt`.
2. **Testing:** Execute `pytest` for Python services and `go test` for the log pipeline.
3. **Docker Build & Push:** On tagging a release, automatically build Docker images for `api_gateway`, `heartbeat_service`, and `log_pipeline`, and push them to GitHub Container Registry (GHCR) or Docker Hub.

## Phase 3: Packaging & Distribution
Users should not have to build from source.
1. **Docker Images:** Make sure public images are available (e.g., `ghcr.io/ashlesh-t/dls-api-gateway:latest`). Update the `docker-compose.yml` to pull these images instead of using `build: .` context for public users.
2. **Helm Chart:** Create a robust Helm chart for Kubernetes users. Publish it to Artifact Hub.
3. **CLI Tool (`dls`):** 
   * Ensure the `dls init` tool mentioned in the README is fully functional.
   * Publish the Homebrew formula to a custom tap (e.g., `ashlesh-t/homebrew-dls`).
   * Publish the AUR package (`dls-cli`) for Arch Linux.

## Phase 4: Documentation
1. **Host a Docs Site:** Move the markdown files in `/docs` to a dedicated documentation site using GitHub Pages (via MkDocs or Docusaurus). 
2. **API Documentation:** Ensure the FastAPI Swagger UI is well-documented with request/response examples.
3. **Quick Start Video/GIF:** Add a GIF to the README showing how fast it is to spin up the stack using `dls start --local`.

## Phase 5: Launch & Marketing
1. **License Check:** Ensure all third-party dependencies are compatible with your MIT license.
2. **Launch Platforms:** Prepare to post the project on platforms like:
   * Hacker News (`Show HN`)
   * Reddit (`r/selfhosted`, `r/kubernetes`, `r/golang`, `r/python`)
   * GitHub Trending (by encouraging early stars from peers).
3. **Release Tags:** Create a formal `v1.0.0` release on GitHub with detailed release notes.
