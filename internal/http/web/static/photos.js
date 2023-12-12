app.controller('controller', ['$scope', 'FMService', '$interval', function ($scope, FMService, $interval) {

    // Get the files array from the directory object
    const files = $scope.directory.files;
    // Filter the files that have the image MIME type
    const photoFiles = files.filter(file => {
      // Check if the file type starts with image/
      return file.type.startsWith("image/");
    });
    // Log the result to the console
    console.log(photoFiles);
    // Log the number of photo files to the console
    console.log("Number of photo files: " + photoFiles.length);
    // Log the names and sizes of the photo files to the console
    photoFiles.forEach(file => {
      console.log("Name: " + file.name + ", Size: " + file.size);
    });
  
    // Log the files array to the console
    console.log("Files array: " + files);
    // Log the photoFiles array to the console
    console.log("PhotoFiles array: " + photoFiles);
    // Use a try-catch block to catch any errors
    try {
      // Your code here ...
    } catch (error) {
      // Log the error to the console
      console.error("Error: " + error.message);
    }
  }]);