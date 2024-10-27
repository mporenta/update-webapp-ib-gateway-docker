# Define your Docker Hub username
$username = "mporenta"

# Define an array of local image names and tags
$images = @(
    "ngrok/ngrok:3.5.0-alpine",
    "redis:7.0.10-alpine",
    "update-webapp-ib-gateway-docker-ib-gateway:latest",
    "update-webapp-ib-gateway-docker-pnl-monitor:latest"
)

# Define an array of remote image names and tags for Docker Hub
$remoteImages = @(
    "$username/ngrok:3.5.0-alpine",
    "$username/redis:7.0.10-alpine",
    "$username/update-webapp-ib-gateway:latest",
    "$username/update-webapp-ib-gateway-pnl-monitor:latest"
)

# Loop over the images to tag and push each one
for ($i = 0; $i -lt $images.Count; $i++) {
    $localImage = $images[$i]
    $remoteImage = $remoteImages[$i]
    
    # Tag the image
    Write-Output "Tagging $localImage as $remoteImage..."
    docker tag $localImage $remoteImage
    
    # Push the image
    Write-Output "Pushing $remoteImage to Docker Hub..."
    docker push $remoteImage
}

Write-Output "All images have been tagged and pushed."
