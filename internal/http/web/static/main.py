# Import the ftplib module
import ftplib

# Create a new FTP instance with the domain and credentials
ftp = ftplib.FTP("localhost", "Shabl", "1212")

# Connect to the FTP server
ftp.connect()

# Change to the desired directory
ftp.cwd("images")

# Get the list of files in the current directory
files = ftp.nlst()

# Filter the files that end with an image format
image_files = [file for file in files if file.endswith((".png", ".jpg", ".jpeg", ".gif"))]

# Loop through the image files
for image_file in image_files:
    # Get the link to the image file
    image_url = f"ftp://{ftp.host}/{image_file}"

    # Load the image in the html file
    # Assuming there is a div element with id="grid" in the html file
    grid = document.getElementById("grid")

    # Create a new image element
    image = document.createElement("img")

    # Set the src attribute to the image url
    image.src = image_url

    # Append the image element to the grid element
    grid.appendChild(image)

# Quit the FTP connection
ftp.quit()