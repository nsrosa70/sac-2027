# cd "$HOME/paper-review-artefact"

# Create fresh history, independent of the original repository.
git init
git branch -M main

# Apply these settings only to this repository.
git config user.name "Anonymous Authors"
git config user.email "anonymous@example.invalid"

git add .
git commit -m "Initial review artifact"

# Copy the repository URL from GitHub.
git remote add origin https://github.com/nsrosa70/sac-2027.git
git push -u origin main
