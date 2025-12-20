#!/usr/bin/env bash

args=("$@")

FLINK_URL="${FLINK_URL:-http://lab.syg:3381}"

SAVEPOINT="file:/usr/share/flink/savepoints/savepoint-f33b0c-acc369cb66b4"
JAR_ID="515de6e2-8a87-4e94-b7be-7c8d5a49c2f3_meshtastic-intelligence-0.1-SNAPSHOT-all.jar"

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

usage() {
  echo "...."
}

case $1 in
  jars)
    list_jars | jq
    ;;
  run)
    run_job_from_savepoint ${JAR_ID} ${SAVEPOINT} "/app/flink-mc.yml"
    ;;
  *)
  usage
esac
