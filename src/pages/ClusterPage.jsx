import React, { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { getCluster, getPhoto } from "../api/api";
import GalleryGrid from "../components/GalleryGrid";

const ClusterPage = () => {
    const { clusterId } = useParams();
    const [photos, setPhotos] = useState([]);
    const [loading, setLoading] = useState(true);
    const navigate = useNavigate();

    useEffect(() => {
        (async () => {
            setLoading(true);
            try {
                const clusterData = await getCluster(clusterId);
                if (clusterData && clusterData.members) {
                    // Fetch full photo details for each member ID
                    const memberPhotos = await Promise.all(
                        clusterData.members.map(async (photoId) => {
                            try {
                                return await getPhoto(photoId);
                            } catch (e) {
                                console.error(`Failed to load photo ${photoId}`, e);
                                return null;
                            }
                        })
                    );
                    setPhotos(memberPhotos.filter(p => p !== null));
                }
            } catch (error) {
                console.error("Failed to load cluster", error);
            } finally {
                setLoading(false);
            }
        })();
    }, [clusterId]);

    return (
        <div className="page">
            <div className="page-header">
                <div className="page-header-left">
                    <button className="back-btn" onClick={() => navigate("/clusters")}>← Back</button>
                    <h1>Cluster {clusterId}</h1>
                </div>
            </div>

            <GalleryGrid
                photos={photos}
                loading={loading}
                activeDelete={null}
            />
        </div>
    );
};

export default ClusterPage;
