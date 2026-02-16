#!/bin/bash

# This script deploys the Datamart Analytics application to the specified environment.

PROJECT_NAME="data-services-datamart-analytics"
PATH_TO_PROJECT="/opt/data_integration"
REPOSITORY="longtermcaregroup@vs-ssh.visualstudio.com:v3/longtermcaregroup/LTCG/data-services-datamart-analytics"
WORKING_DIR="$PATH_TO_PROJECT/$PROJECT_NAME"
REPOSITORY_BRANCH="release"

function verify_repository() {
    # Verify if the repository exists in the specified path
    # If not, clone it from the remote repository.
    
    echo "Checking if repository $PROJECT_NAME exists in $PATH_TO_PROJECT..."
    
    if [ ! -d "$WORKING_DIR" ]; then
        # If the repository does not exist, create it.
        echo "Repository path '$WORKING_DIR' does not exist."
        
        git clone "$REPOSITORY" "$WORKING_DIR"
        
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to clone repository from $REPOSITORY."
            exit 1
        fi
    else
        echo "Repository $PROJECT_NAME already exists in $PATH_TO_PROJECT."
        cd "$WORKING_DIR" || exit 1
    fi
    
    # Check if the repository is valid
    if ! git status > /dev/null 2>&1; then
        echo "ERROR: Not a valid git repository at $WORKING_DIR."
        exit 1
    else
        echo "Valid git repository found at $WORKING_DIR."
        
        echo "Current directory: $(pwd)"
        echo "Current branch: $(git branch --show-current)"
        echo "Current commit: $(git log -1 --oneline)"
        echo "Repository status: $(git status --porcelain | wc -l) uncommitted changes"
    fi
}

function deploy_project() {
    # Deploy the project by pulling the latest changes from the specified branch.
    
    echo "Deploying $PROJECT_NAME from branch $(git branch --show-current)"
    
    STASH_NAME="Auto-stash before deployment $(date +%Y-%m-%d_%H-%M-%S)"
    
    # Stash any local changes
    if [ -n "$(git status --porcelain)" ]; then
        echo "Stashing local changes"
        git stash push -m "$STASH_NAME"
    fi
    
    # Fetch the latest changes from the remote repository
    git fetch origin
    
    # Check if the specified branch exists on the remote
    if git show-ref --verify --quiet refs/remotes/origin/$REPOSITORY_BRANCH; then
        echo "Branch $REPOSITORY_BRANCH exists. Checking out and pulling latest changes."
        git checkout $REPOSITORY_BRANCH
        git pull origin $REPOSITORY_BRANCH
    else
        echo "ERROR: Branch $REPOSITORY_BRANCH does not exist on the remote repository."
        exit 1
    fi
    
    # Apply stashed changes if any
    if git stash list | grep -q "$STASH_NAME"; then
        echo "Applying stashed changes"
        git stash pop
    fi
    
    echo "Updated to branch $REPOSITORY_BRANCH"
    echo "Current commit: $(git log -1 --oneline)"
    echo "Repository status: $(git status --porcelain | wc -l) uncommitted changes"
}

# Main script execution
# 1st verify the repository in the target server
verify_repository
# 2nd deploy the project
deploy_project
