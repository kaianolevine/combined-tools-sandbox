# Python Project Template

This is a reusable template for Python projects using Poetry, pytest, pre-commit, and GitHub Actions.

## 🔧 Features
- Poetry for dependency management
- pytest for testing
- Pre-commit hooks for code linting/formatting
- GitHub Actions for CI
- Sample test file
- Ready for VSCode

---

## 🚀 Getting Started

### 1. Create a New Repo Based on This Template
#### Option A: GitHub
1. Push this template to a GitHub repository.
2. In the GitHub repo, go to **Settings** → **"Template repository"** → Enable it.
3. Click **"Use this template"** to create new projects.

#### Option B: Manual
```bash
git clone https://github.com/your-username/python-project-template.git your-new-project
cd your-new-project
rm -rf .git
git init
git remote add origin https://github.com/your-username/your-new-project.git
git add .
git commit -m "Initial commit from template"
git push -u origin main
```

---

### 2. Set Up Locally
```bash
poetry install
poetry shell
pre-commit install
```

### 3. Run Tests
```bash
pytest
```

---

## ✅ Customize
Replace this README and add your own code under a new `your_package/` directory.
