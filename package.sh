poetry export -f requirements.txt -o requirements.txt --without-hashes
pip3 install -t ./upload/src -r requirements.txt
cp -r ./stock_evaluator ./upload/src
cd ./upload/src
zip -r stock_eval_.zip .