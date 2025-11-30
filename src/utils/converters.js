// Convert hex → base64 (for / and /getSimilar)
export const hexToBase64 = (hex) => {
  if (!hex) return null;

  const bytes = new Uint8Array(
    hex.match(/.{1,2}/g).map((b) => parseInt(b, 16))
  );

  let binary = "";
  bytes.forEach((b) => (binary += String.fromCharCode(b)));

  return btoa(binary);
};

// Decide proper image source automatically
export const getImageSource = (photo) => {
  if (photo.base64)
    return "data:image/jpeg;base64," + photo.base64;

  if (photo.binaryData)
    return "data:image/jpeg;base64," + hexToBase64(photo.binaryData);

  return null;
};