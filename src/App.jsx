import React from "react";
import { Routes, Route, Link } from "react-router-dom";
import HomePage from "./pages/HomePage";
import SimilarPhotosPage from "./pages/SimilarPhotosPage";

const App = () => {
  return (
    <div className="app-root">
      <header className="app-header">
        <div className="app-header-inner">
          <Link to="/" className="brand">
            PicStack
          </Link>
          <nav className="nav-links">
            <Link to="/">Home</Link>
          </nav>
        </div>
      </header>

      <main className="app-main">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/similar/:photoId" element={<SimilarPhotosPage />} />
        </Routes>
      </main>
    </div>
  );
};

export default App;
