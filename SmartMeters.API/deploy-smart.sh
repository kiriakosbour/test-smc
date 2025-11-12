#!/bin/bash
# ===========================================
# SmartMeters.API full build & deploy script
# ===========================================

# Paths
PROJECT_DIR="/Users/akyritopoulos/IdeaProjects/SmartMeters.API"
WILDFLY_HOME="/Users/akyritopoulos/IdeaProjects/smart/wildfly-26.1.3.Final"
WAR_NAME="load-profile-push-api.war"
TARGET_WAR="$PROJECT_DIR/target/$WAR_NAME"
DEPLOY_DIR="$WILDFLY_HOME/standalone/deployments"

# Step 1. Clean and build
echo "🧱 Building project with Maven..."
cd "$PROJECT_DIR" || exit
mvn clean package -DskipTests
if [ $? -ne 0 ]; then
  echo "❌ Build failed!"
  exit 1
fi

# Step 2. Stop any running WildFly
echo "🛑 Stopping existing WildFly..."
pkill -f "wildfly-26.1.3.Final" && sleep 2

# Step 3. Deploy new .war
echo "🚚 Copying WAR to deployments..."
rm -f "$DEPLOY_DIR/$WAR_NAME"*
cp "$TARGET_WAR" "$DEPLOY_DIR/"

# Step 4. Start WildFly
echo "🚀 Starting WildFly..."
cd "$WILDFLY_HOME/bin" || exit
nohup ./standalone.sh > "$WILDFLY_HOME/standalone/log/server.log" 2>&1 &
sleep 6

# Step 5. Quick status check
echo "🌐 Checking /ping endpoint..."
curl -s http://localhost:8080/load-profile-push-api/api/load-profile/ping || echo "⚠️ Ping failed (server still starting?)"

# Step 6. Tail logs
echo "📜 Showing server log (Ctrl+C to stop)"
tail -f "$WILDFLY_HOME/standalone/log/server.log"

