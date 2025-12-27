#!/usr/bin/env bash

args=("$@")

FLINK_URL="${FLINK_URL:-http://lab.syg:3381}"

SAVEPOINT="file:/usr/share/flink/savepoints/savepoint-903c52-62337e91e06f"
JAR_ID="${JAR_ID}"

list_jars() {
  local jars

  jars=$(curl -sf "${FLINK_URL}/jars") || {
    echo "❌ curl failed" >&2
    return 1
  }

#    local jars
#
#    jars=$(curl -sf "${FLINK_URL}/jars") || {
#      echo "❌ curl failed" >&2
#      return 1
#    }
    local jar_id
#    jar_id=$(echo "${jars}" | jq '.files | sort_by(.uploaded) | last? | .id')


    jar_id=$(parse_jar_id "${jars}")
    jar_id=${jar_id//\"/}

    echo "$jar_id"



#  echo "$jars" | jq -r '
#    .files // []
#    | map(select(any(.entry[]?; .name == "s7i.flink.IntelligenceJob")))
#    | sort_by(.uploaded)
#    | last?
#    | .id // empty
#  '
}

parse_jar_id() {
    echo "$1" \
    | tee >(sed 's/^/[STEP 1] /' >&2) \
    | jq '.files' \
    | tee >(sed 's/^/[STEP 2] /' >&2) \
    | jq ' sort_by(.uploaded) | last?' \
    | tee >(sed 's/^/[STEP 3] /' >&2) \
    | jq ' .id '

}


run_job_from_savepoint() {
  local JAR_ID="$1"
  local ENTRY_CLASS="s7i.flink.IntelligenceJob"
  local SAVEPOINT_PATH="$2"
  local PROGRAM_ARGS="--mesh-cfg $3"
  local PARALLELISM="1"

  echo "▶ Starting job from savepoint..."

  local PAYLOAD
  PAYLOAD=$(jq -n \
    --arg entryClass "$ENTRY_CLASS" \
    --arg programArgs "$PROGRAM_ARGS" \
    --arg savepointPath "$SAVEPOINT_PATH" \
    --argjson parallelism "$PARALLELISM" \
    '{
      entryClass: $entryClass,
      programArgs: ($programArgs | select(length > 0)),
      parallelism: $parallelism,
      savepointPath: $savepointPath
    }')

  curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD" \
    "${FLINK_URL}/jars/${JAR_ID}/run" | jq
}

upload_jar() {
  JAR_PATH="./build/libs/meshtastic-intelligence-0.1-SNAPSHOT-all.jar"
  echo "▶ Uploading JAR..."
  UPLOAD_RESPONSE=$(curl -s -X POST \
    -H "Expect:" \
    -F "jarfile=@${JAR_PATH}" \
    "${FLINK_URL}/jars/upload")

  JAR_ID=$(echo "$UPLOAD_RESPONSE" | jq -r '.filename' | sed 's|.*/||')
  echo "jarid: ${JAR_ID}"
}

usage() {
  echo "...."
}

case $1 in
  jars)
    list_jars | jq
    ;;
  run)
    [[ -z "${JAR_ID}" ]] && echo "missing jar" && exit 1
    run_job_from_savepoint ${JAR_ID} ${SAVEPOINT} "/app/flink-mc.yml"
    ;;
  upload)
    upload_jar
    ;;
  *)
  usage
esac
