import React, { useState } from "react";
import { getImageSource } from "../utils/converters";

const PhotoCard = ({ photo, onClick, onRightClickDelete, activeDelete }) => {
  const [loaded, setLoaded] = useState(false);
  const imgSrc = getImageSource(photo);

  const handleContextMenu = (e) => {
    e.preventDefault();
    e.stopPropagation();
    onRightClickDelete(photo);
  };

  return (
    <div className="photo-card" onContextMenu={handleContextMenu}>
      <div className={`photo-inner ${loaded ? "loaded" : ""}`}>
        <img
          src={imgSrc}
          alt={photo.photoId}
          className="photo-img"
          onLoad={() => setLoaded(true)}
          onClick={() => onClick(photo)}
        />

        {activeDelete === photo.photoId && (
          <button
            className="delete-button"
            onClick={(e) => {
              e.stopPropagation();
              onRightClickDelete(photo, true);
            }}
          >
            Delete
          </button>
        )}
      </div>
    </div>
  );
};

export default PhotoCard;