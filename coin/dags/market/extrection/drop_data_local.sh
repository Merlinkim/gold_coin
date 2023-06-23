#!/bin/zsh

ROUTE="$1"

FILE="$2"

LOOP_VARIABLE=""

LOOP_VARIABLE=$(cat "$FILE")

for NAME in ${LOOP_VARIABLE};
do
	mkdir -p "${ROUTE}/${NAME}"
	curl --request GET \
     	 --url 'https://api.upbit.com/v1/candles/minutes/1?market='${NAME}'&count=1' \
         --header 'accept: application/json' >> ${ROUTE}/${NAME}/NAME.JSON
done
