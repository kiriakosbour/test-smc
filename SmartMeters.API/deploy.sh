#!/bin/bash

# === CONFIGURATION ===
PROJECT_DIR=~/IdeaProjects/SmartMeters.API
WILDFLY_HOME=~/Downloads/wildfly-32.0.1.Final
WAR_NAME=load-profile-push-api.war

# === STEP 1: Build the project ===
echo "🔨 Building Maven project..."
cd "$PROJECT_DIR" || exit 1
mvn clean package -q
if [ $? -ne 0 ]; then
  echo "❌ Maven build failed!"
  exit 1
fi
echo "✅ Build completed successfully!"

# === STEP 2: Deploy WAR to WildFly ===
echo "📦 Deploying WAR to WildFly..."
cp "$PROJECT_DIR/target/$WAR_NAME" "$WILDFLY_HOME/standalone/deployments/"
if [ $? -ne 0 ]; then
  echo "❌ Failed to copy WAR file to WildFly deployments folder!"
  exit 1
fi
echo "✅ WAR deployed to $WILDFLY_HOME/standalone/deployments/"

# === STEP 3: Restart WildFly ===
echo "♻️ Restarting WildFly..."
"$WILDFLY_HOME/bin/jboss-cli.sh" --connect command=:shutdown > /dev/null 2>&1
sleep 3
"$WILDFLY_HOME/bin/standalone.sh" > "$WILDFLY_HOME/standalone/log/deploy.log" 2>&1 &
sleep 5
echo "✅ WildFly restarted successfully!"

# === STEP 4: Tail the server log ===
echo "📜 Tailing WildFly log..."
tail -n 30 -f "$WILDFLY_HOME/standalone/log/deploy.log"

