import subprocess

# List of paths to your existing scripts on GitHub
scripts = [
    'https://raw.githubusercontent.com/aurelpow/nba_project_stats/main/NBA_scrap1_seasons.py',
    'https://raw.githubusercontent.com/aurelpow/nba_project_stats/main/NBA_test_model.py'
]

def execute_scripts():
    for script in scripts:
        print(f"Executing script: {script}")
        try:
            subprocess.run(['python', '-u', script], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error executing script {script}: {e}")
        print("")

if __name__ == "__main__":
    execute_scripts()
