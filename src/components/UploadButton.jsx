import React, { useRef } from "react";

export default function UploadButton({ onFileSelected }) {
  const ref = useRef(null);
  return (
    <>
      <button className="upload-btn" onClick={() => ref.current.click()}>+</button>
      <input
        type="file"
        accept="image/*"
        ref={ref}
        style={{ display: "none" }}
        onChange={(e) => e.target.files[0] && onFileSelected(e.target.files[0])}
      />
    </>
  );
}
