export MSSQL_UID=sa
export MSSQL_PWD='ThePa$$word'
docker run --name test_mssql_server -e 'ACCEPT_EULA=Y' -e SA_PASSWORD=$MSSQL_PWD -p 1433:1433 -d microsoft/mssql-server-linux
