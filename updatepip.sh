python setup.py sdist bdist_wheel
yes | pip uninstall ticdat
yes | pip install dist/ticdat-0.2.5.tar.gz
cp dist/ticdat-0.2.5.tar.gz ../opalytics-lenticular/lenticular
cp -rf ticdat/* ../opalytics-lenticular/lenticular/oplstuff/ticdat
