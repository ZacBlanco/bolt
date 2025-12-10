#!/usr/bin/env python3
import os
import subprocess
import requests

def create_registration_token(token, org_name, repo_name):
    url = f"https://api.github.com/repos/{org_name}/{repo_name}/actions/runners/registration-token"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]

def main():
    # ensure all variables are set, error message should include unset variables
    unset_vars = [
        var
        for var in [
            "GITHUB_RUNNER_TOKEN",
            "ORGANIZATION_NAME",
            "REPOSITORY_NAME",
            "RUNNER_LABELS",
            "DOCKER_DATA_DIR"
        ]
        if not os.environ.get(var)
    ]
    if unset_vars:
        raise ValueError(f"Variables {', '.join(unset_vars)} must be set")

    # do a reverse DNS lookup to get the hostname:
    runner_hostname = subprocess.check_output("dig -x $(hostname -i) +short | cut -d'.' -f1", shell=True).strip()

    gh_auth_token = os.environ["GITHUB_RUNNER_TOKEN"]
    runner_name = f"{os.environ['RUNNER_HOSTNAME']}-{runner_hostname}"
    org_name = os.environ["ORGANIZATION_NAME"]
    repo_name = os.environ["REPOSITORY_NAME"]
    runner_labels = os.environ["RUNNER_LABELS"]
    docker_data_dir = os.environ["DOCKER_DATA_DIR"]
    
    # allow running as root
    os.environ["RUNNER_ALLOW_RUNASROOT"] = "1"
    
    # starting docker - each runner replica needs a unique /var/lib/docker directory
    # compose isn't capable of editing mounts after startup, so we need to create
    # the directory and symlink it to /var/lib/docker
    os.system(f"mkdir -p {docker_data_dir}/{runner_hostname}")
    os.system(f"ln -s {docker_data_dir}/{runner_hostname} /var/lib/docker")
    os.system("service docker start")
    
    # create a registration token for the runner
    registration_token = create_registration_token(gh_auth_token, org_name, repo_name)
    print(f"Configuring the runner with name {runner_name}")
    config_command = f"/actions-runner/config.sh --url https://github.com/{org_name}/{repo_name} --token {registration_token} --name {runner_name} --replace --labels {runner_labels} --unattended"
    os.system(config_command)
    
    print("Starting the runner...")
    os.execv("/bin/bash", ["/bin/bash", "/actions-runner/run.sh"])

if __name__ == "__main__":
    main()
