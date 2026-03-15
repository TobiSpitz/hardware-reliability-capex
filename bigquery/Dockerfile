FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# APP_MODE controls which Flask app to run: "dashboard", "dashboard_v2", or "review"
ENV APP_MODE=dashboard
ENV GCS_BUCKET=""
ENV PORT=8080

EXPOSE 8080

CMD if [ "$APP_MODE" = "review" ]; then \
      exec python -m gunicorn station_review_app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120; \
    else \
      exec python -m gunicorn capex_dashboard:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120; \
    fi
