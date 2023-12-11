// Import the jsftp library
import jsftp from "../../../../node_modules/jsftp/index.js";


// Create a new FTP instance with the domain and credentials
const ftp = new jsftp({
  host: "localhost",
  user: "Shabl",
  pass: "1212"
});

// Connect to the FTP server
ftp.connect(err => {
  if (err) {
    // Handle connection errors
    console.error(err);
  } else {
    // List the files in the current directory
    ftp.ls(".", (err, files) => {
      if (err) {
        // Handle listing errors
        console.error(err);
      } else {
        // Log the files to the console
        console.log(files);

        // Filter the files that end with an image format
        const imageFiles = files.filter(file => file.name.match(/\.(png|jpg|jpeg|gif)$/i));

        // Loop through the image files
        for (let imageFile of imageFiles) {
          // Get the link to the image file
          const imageUrl = `ftp://${ftp.host}/${imageFile.name}`;

          // Load the image in the html file
          // Assuming there is a div element with id="grid" in the html file
          const grid = document.getElementById("grid");

          // Create a new image element
          const image = document.createElement("img");

          // Set the src attribute to the image url
          image.src = imageUrl;

          // Append the image element to the grid element
          grid.appendChild(image);
        }
      }
    });
  }
});