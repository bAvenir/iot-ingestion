# M0 — one image, entrypoint chosen by compose `command`:
#   api     -> uvicorn app.api.main:app --host 0.0.0.0
#   bronze  -> python -m app.workers.bronze
#   silver  -> python -m app.workers.silver
#   publish -> python -m app.workers.publish
#
# bvr-ci passes VERSION, COMMIT_SHA, BUILD_TIME as build-args automatically:
# ARG VERSION=unknown
# ARG COMMIT_SHA=unknown
# ARG BUILD_TIME=unknown
# ENV VERSION=$VERSION COMMIT_SHA=$COMMIT_SHA BUILD_TIME=$BUILD_TIME
