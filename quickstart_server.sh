#!/usr/bin/env bash

if [ -z "$VIRTUAL_ENV" ]; then
  echo "Starting virtual env"
  . ./.venv/bin/activate
fi

cd ./web/
python manage.py runserver