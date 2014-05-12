echo "Starting Stratosphere"

. ./config.sh

if [[ $YARN == "true" ]]; then
	$STRATOSPHERE_BUILD_HOME/bin/yarn-session.sh $YARN_SESSION_CONF
else
	$STRATOSPHERE_BUILD_HOME/bin/start-cluster.sh
fi
