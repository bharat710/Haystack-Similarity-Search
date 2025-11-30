import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import GalleryGrid from "../components/GalleryGrid";
import UploadButton from "../components/UploadButton";
import { uploadPhoto, deletePhoto, getPhoto } from "../api/api";
import { getImageSource } from "../utils/converters";

const PHOTO_IDS_KEY = "photoIds";

const HomePage = () => {
  const [photos, setPhotos] = useState([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [deleteId, setDeleteId] = useState(null);

  const navigate = useNavigate();

  useEffect(() => {
    const hideDelete = () => setDeleteId(null);
    window.addEventListener("click", hideDelete);
    return () => window.removeEventListener("click", hideDelete);
  }, []);

  useEffect(() => {
    const fetchPhotos = async () => {
      console.log("HomePage mounted. Loading photos...");
      setLoading(true);
      try {
        const storedIds =
          JSON.parse(localStorage.getItem(PHOTO_IDS_KEY)) || [];
        console.log("Stored photo IDs from localStorage:", storedIds);

        if (storedIds.length > 0) {
          const photoPromises = storedIds.map(getPhoto);
          const loadedPhotos = await Promise.all(photoPromises);
          console.log("Loaded photos from API:", loadedPhotos);
          setPhotos(loadedPhotos.filter((p) => p)); // Filter out any nulls if a photo fetch failed
        }
      } catch (error) {
        console.error("Error loading photos from storage:", error);
      } finally {
        setLoading(false);
        console.log("Finished loading photos.");
      }
    };

    fetchPhotos();
  }, []);

  const handleRightClickDelete = async (photo, confirm = false) => {
    if (!confirm) {
      setDeleteId(photo.photoId);
      return;
    }

    console.log("Deleting photo:", photo.photoId);
    setDeleteId(null);
    const prevPhotos = photos;
    setPhotos(prevPhotos.filter((p) => p.photoId !== photo.photoId));

    try {
      await deletePhoto(photo.photoId);
      const storedIds = JSON.parse(localStorage.getItem(PHOTO_IDS_KEY)) || [];
      const newIds = storedIds.filter((id) => id !== photo.photoId);
      console.log("Updating stored photo IDs:", newIds);
      localStorage.setItem(PHOTO_IDS_KEY, JSON.stringify(newIds));
    } catch (_) {
      console.error("Failed to delete photo, reverting UI.", _);
      setPhotos(prevPhotos);
    }
  };

  return (
    <div className="page">
      <div className="page-header">
        <h1>Home</h1>
        {uploading && <span className="upload-status">Uploading...</span>}
      </div>
      {console.log("From homepage - Photos object:", photos)}
      <GalleryGrid
        photos={photos}
        loading={loading}
        onRightClickDelete={handleRightClickDelete}
        activeDelete={deleteId}
      />

      <UploadButton
        onFileSelected={async (file) => {
          setUploading(true);
          console.log("Starting file upload...");
          try {
            const uploadResult = await uploadPhoto(file);
            console.log("Upload API result:", uploadResult);

            if (!uploadResult || !uploadResult.photoId) {
              throw new Error("Upload failed: No photoId returned");
            }

            console.log("Fetching uploaded photo data...");
            const fetched = await getPhoto(uploadResult.photoId);
            console.log("Fetched photo data:", fetched);

            setPhotos((prev) => [fetched, ...prev]);

            const storedIds =
              JSON.parse(localStorage.getItem(PHOTO_IDS_KEY)) || [];
            const newIds = [fetched.photoId, ...storedIds];
            console.log("Updating stored photo IDs with new photo:", newIds);
            localStorage.setItem(PHOTO_IDS_KEY, JSON.stringify(newIds));
          } catch (error) {
            console.error("Upload error:", error);
            alert(`Upload failed: ${error.message}`);
          } finally {
            setUploading(false);
            console.log("Finished file upload process.");
          }
        }}
      />
    </div>
  );
};

export default HomePage;