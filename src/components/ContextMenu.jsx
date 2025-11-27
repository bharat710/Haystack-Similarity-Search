import React from "react";

const ContextMenu = ({ visible, x, y, onDelete, onClose }) => {
  if (!visible) return null;

  const handleOverlayClick = () => {
    if (onClose) onClose();
  };

  const handleMenuClick = (e) => {
    e.stopPropagation(); // don't close when clicking inside
  };

  return (
    <div className="context-menu-overlay" onClick={handleOverlayClick}>
      <div
        className="context-menu"
        style={{ top: y, left: x }}
        onClick={handleMenuClick}
      >
        <button className="context-menu-item context-menu-danger" onClick={onDelete}>
          Delete photo
        </button>
      </div>
    </div>
  );
};

export default ContextMenu;