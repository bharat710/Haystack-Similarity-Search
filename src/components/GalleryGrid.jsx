import React from "react";
import Masonry from "react-masonry-css";
import PhotoCard from "./PhotoCard";
import SkeletonCard from "./SkeletonCard";

const breakpointColumns = {
  default: 4,
  1200: 3,
  800: 2,
  500: 1,
};

const GalleryGrid = ({
  photos,
  loading,
  onPhotoClick,
  onRightClickDelete,
  activeDelete,
}) => {
  if (loading) {
    return (
      <div className="gallery-container">
        <Masonry
          breakpointCols={breakpointColumns}
          className="masonry-grid"
          columnClassName="masonry-column"
        >
          {Array.from({ length: 8 }).map((_, idx) => (
            <SkeletonCard key={idx} />
          ))}
        </Masonry>
      </div>
    );
  }

  return (
    <div className="gallery-container">
      <Masonry
        breakpointCols={breakpointColumns}
        className="masonry-grid"
        columnClassName="masonry-column"
      >
        {photos.map((p) => (
          <PhotoCard
            key={p.photoId}
            photo={p}
            onClick={onPhotoClick}
            onRightClickDelete={onRightClickDelete}
            activeDelete={activeDelete}
          />
        ))}
      </Masonry>
    </div>
  );
};

export default GalleryGrid;