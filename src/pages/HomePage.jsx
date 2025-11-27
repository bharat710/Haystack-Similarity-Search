import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import GalleryGrid from "../components/GalleryGrid";
import UploadButton from "../components/UploadButton";
import { getImages, uploadPhoto, deletePhoto, getPhoto } from "../api/api";

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
    (async () => {
      setLoading(true);
      const data = await getImages();
      setPhotos(data || []);
      setLoading(false);
    })();
  }, []);

  const handleRightClickDelete = async (photo, confirm = false) => {
    if (!confirm) {
      setDeleteId(photo.photoId);
      return;
    }

    setDeleteId(null);
    const prev = photos;
    setPhotos(prev.filter((p) => p.photoId !== photo.photoId));

    try {
      await deletePhoto(photo.photoId);
    } catch (_) {
      setPhotos(prev);
    }
  };

  return (
    <div className="page">
      <div className="page-header">
        <h1>Home</h1>
        {uploading && <span className="upload-status">Uploading...</span>}
      </div>

      <GalleryGrid
        photos={photos}
        loading={loading}
        onPhotoClick={(p) => navigate(`/similar/${p.photoId}`)}
        onRightClickDelete={handleRightClickDelete}
        activeDelete={deleteId}
      />

      <UploadButton
        onFileSelected={async (file) => {
          setUploading(true);
          try {
            // Phase 1 upload (safe retry + idempotency)
            const uploadResult = await uploadPhoto(file);

            // Phase 2 fetch committed file
            const fetched = await getPhoto(uploadResult.photoId);

            setPhotos((prev) => [
              {
                photoId: fetched.photoId,
                base64: fetched.base64
              },
              ...prev
            ]);
          } finally {
            setUploading(false);
          }
        }}
      />
    </div>
  );
};

export default HomePage;