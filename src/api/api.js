import axios from "axios";

// Generate UUID idempotency key
export const generateIdempotencyKey = () =>
  crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + ":" + Math.random();

const apiClient = axios.create({
  baseURL: "http://10.1.35.178:8000",
});

// GET single photo → /photos/{photo_id}
export const getPhoto = async (photoId) => {
  try {
    const res = await apiClient.get(`/photos/${photoId}`);
    const photoData = {
      photoId: res.data.photo_id,
      base64: res.data.data,
    };
    console.log(`getPhoto(${photoId}) returned:`, photoData);
    return photoData;
  } catch (error) {
    console.error(`Error fetching photo with ID ${photoId}:`, error);
    return null;
  }
};

export const getClusters = async () => {
  const res = await apiClient.get(`/clusters`);
  return res.data;
};

export const getCluster = async (cluster_id) => {
  const res = await apiClient.get(`/cluster/${cluster_id}`);
  return res.data;
};

export const deletePhoto = async (photoId) => {
  const res = await apiClient.delete(`/photos/${photoId}`);
  return {
    photoId: res.data.photo_id,
    base64: res.data.data
  };
};

// UPLOAD → /upload_photo/
export const uploadPhoto = async (file) => {
  const idempotencyKey =
    typeof crypto !== "undefined" && crypto.randomUUID
      ? crypto.randomUUID()
      : String(Date.now()) + ":" + Math.random();

  const form = new FormData();
  form.append("file", file);

  console.log("Sending upload request to /upload_photo/", {
    fileName: file.name,
    fileSize: file.size,
    idempotencyKey,
  });

  try {
    const res = await apiClient.post("/upload_photo/", form, {
      headers: {
        // "Content-Type": "multipart/form-data",
        "idempotency-key": idempotencyKey,
      },
    });

    console.log("Upload response:", res);

    return {
      status: res.data.status,
      photoId: res.data.photo_id,
      idempotencyKey,
    };
  } catch (error) {
    console.error("API upload error:", error);
    throw error;
  }
};