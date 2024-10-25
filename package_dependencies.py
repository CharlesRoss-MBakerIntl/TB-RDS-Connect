import os
import subprocess
import zipfile

def create_venv(venv_name):
    subprocess.run(['python', '-m', 'venv', venv_name])
    if os.name == 'nt':
        activate_script = os.path.join(venv_name, 'Scripts', 'activate.bat')
    else:
        activate_script = os.path.join(venv_name, 'bin', 'activate')
    return activate_script

def install_dependencies(venv_name):
    requirements_file = 'requirements.txt'
    if os.name == 'nt':
        pip_executable = os.path.join(venv_name, 'Scripts', 'pip')
    else:
        pip_executable = os.path.join(venv_name, 'bin', 'pip')
    subprocess.run([pip_executable, 'install', '-r', requirements_file])

def package_lambda(deployment_package_path, venv_name):
    if os.name == 'nt':
        site_packages_path = os.path.join(venv_name, 'Lib', 'site-packages')
    else:
        site_packages_path = os.path.join(venv_name, 'lib', f'python{os.sys.version_info.major}.{os.sys.version_info.minor}', 'site-packages')
    
    with zipfile.ZipFile(deployment_package_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(site_packages_path):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), site_packages_path))
        zipf.write('your_script.py')

if __name__ == "__main__":
    venv_name = 'myenv'
    deployment_package_path = 'deployment-package.zip'

    activate_script = create_venv(venv_name)
    install_dependencies(venv_name)
    package_lambda(deployment_package_path, venv_name)

    print(f'Deployment package {deployment_package_path} created successfully.')
