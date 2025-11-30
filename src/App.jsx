import React from "react";
import { Routes, Route, Link } from "react-router-dom";
import HomePage from "./pages/HomePage";
import ClustersPage from "./pages/ClustersPage";
import ClusterPage from "./pages/ClusterPage";

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
            <Link to="/clusters">Clusters</Link>
          </nav>
        </div>
      </header>

      <main className="app-main">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/clusters" element={<ClustersPage />} />
          <Route path="/cluster/:clusterId" element={<ClusterPage />} />
        </Routes>
      </main>
    </div>
  );
};

export default App;
