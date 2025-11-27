import React, { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import GalleryGrid from "../components/GalleryGrid";
import { getSimilarImages, deletePhoto } from "../api/api";

const SimilarPhotosPage = () => {
  const { photoId } = useParams();
  const [photos, setPhotos] = useState([]);
  const [loading, setLoading] = useState(true);
  const [deleteId, setDeleteId] = useState(null);

  const navigate = useNavigate();

  useEffect(() => {
    const handler = () => setDeleteId(null);
    window.addEventListener("click", handler);
    return () => window.removeEventListener("click", handler);
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setPhotos((await getSimilarImages(photoId)) || []);
      setLoading(false);
    })();
  }, [photoId]);

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
    } catch {
      setPhotos(prev);
    }
  };

  return (
    <div className="page">
      <div className="page-header">
        <div className="page-header-left">
          <button className="back-btn" onClick={() => navigate(-1)}>← Back</button>
          <h1>Similar Photos</h1>
        </div>
        <span className="photo-id-label">Source: {photoId}</span>
      </div>

      <GalleryGrid
        photos={photos}
        loading={loading}
        onPhotoClick={(p) => navigate(`/similar/${p.photoId}`)}
        onRightClickDelete={handleRightClickDelete}
        activeDelete={deleteId}
      />
    </div>
  );
};

export default SimilarPhotosPage;