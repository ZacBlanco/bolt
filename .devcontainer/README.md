# Bolt Devcontainer Setup

This guide explains how to build and configure the Bolt devcontainer.

## Building the Devcontainer

### Using VS Code (Recommended)

1. Open the project in Visual Studio Code
2. Install the [Remote - Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
3. Click the green "Open a Remote Window" button in the lower-left corner
4. Select "Reopen in Container" from the menu

### Using Docker CLI

```bash
# From the project root directory
cd .github/runners && docker build -f .github/runners/Dockerfile.ci -t bolt-devcontainer:latest .
```

## Building Worldwide and in Corporate Networks

### DEB_REGION

The `DEB_REGION` argument allows you to specify a Debian mirror region to use for faster package downloads.

**Example values:**
- `us` (United States)
- `uk` (United Kingdom)
- `cn` (China)

Debian's default package mirrors at deb.debian.org are supposed to automatically
redirect your client to the closest mirror when building the package. However,
sometime it doesn't work. Set the `DEB_REGION` image build argument if the
download of debian packages is too slow.

**Usage with VS Code:**
1. Edit `.devcontainer/devcontainer.json`
2. Add the build argument to the `build.args` section:
   ```json
   "build": {
     "args": {
       "DEB_REGION": "us"
     }
   }
   ```

**Usage with Docker CLI:**
```bash
docker build -f .github/runners/Dockerfile.ci --build-arg DEB_REGION="us" -t bolt-devcontainer:latest .
```

### HTTPS_PROXY

The `HTTPS_PROXY` argument allows you to specify an HTTPS proxy for
network connections if you're behind a firewall.

**Example value:**
- `http://proxy.company.com:8080`

**Usage with VS Code:**
1. Edit `.devcontainer/devcontainer.json`
2. Add the build argument to the `build.args` section:
   ```json
   "build": {
     "args": {
       "HTTPS_PROXY": "http://proxy.company.com:8080"
     }
   }
   ```

**Usage with Docker CLI:**
```bash
docker build -f .github/runners/Dockerfile.ci --build-arg HTTPS_PROXY="http://proxy.company.com:8080" -t bolt-devcontainer:latest .
```