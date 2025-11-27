import axios from "axios";

// Generate UUID idempotency key
export const generateIdempotencyKey = () =>
  crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + ":" + Math.random();

const apiClient = axios.create({
  baseURL: "http://10.1.35.178:8000",
});

// GET ALL → /
export const getImages = async () => {
  const res = await apiClient.get("/");
  return [];
};

// GET single photo → /photos/{photo_id}
export const getPhoto = async (photoId) => {
  const res = await apiClient.get(`/photos/${photoId}`);
  // console.log( {
  //   photoId: res.data.photo_id,
  //   base64: res.data.data
  // })
  return {
    photoId: res.data.photo_id,
    base64: res.data.data
  };
};

export const deletePhoto = async (photoId) => {
  const res = await apiClient.delete(`/photos/${photoId}`);
  // console.log( {
  //   photoId: res.data.photo_id,
  //   base64: res.data.data
  // })
  return {
    photoId: res.data.photo_id,
    base64: res.data.data
  };
};

// UPLOAD → /upload_photo/
export const uploadPhoto = async (file) => {
  const idempotencyKey = generateIdempotencyKey();

  const form = new FormData();
  form.append("file", file);

  const res = await apiClient.post("/upload_photo/", form, {
    headers: {
      // "Content-Type": "multipart/form-data",
      "idempotency-key": idempotencyKey
    }
  });

  return {
    status: res.data.status,
    photoId: res.data.photo_id,
    idempotencyKey
  };
};

// SIMILAR (unchanged)
export const getSimilarImages = async (photoId) => {
  const res = await apiClient.get("/getSimilar", {
    params: { photoId },
  });
  return res.data;
};

// // DELETE (unchanged)
// export const deletePhoto = async (photoId) => {
//   const res = await apiClient.delete("/deletePhoto", {
//     params: { photoId }
//   });
//   return res.data;
// };