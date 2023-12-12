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
}])