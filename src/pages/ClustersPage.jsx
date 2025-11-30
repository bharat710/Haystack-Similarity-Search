import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getClusters, getPhoto } from "../api/api";
import GalleryGrid from "../components/GalleryGrid";

const ClustersPage = () => {
    const [photos, setPhotos] = useState([]);
    const [loading, setLoading] = useState(true);
    const navigate = useNavigate();

    useEffect(() => {
        (async () => {
            try {
                const data = await getClusters();

                const enriched = await Promise.all(
                    (data || []).map(async (c) => {
                        try {
                            const photo = await getPhoto(c.representative_photo_id);
                            return {
                                photoId: c.cluster_id,
                                base64: photo?.base64,
                                clusterMeta: c,
                            };
                        } catch (e) {
                            return {
                                photoId: c.cluster_id,
                                base64: null,
                                clusterMeta: c,
                            };
                        }
                    })
                );

                setPhotos(enriched);
            } catch (e) {
                console.error("Failed to load clusters", e);
            } finally {
                setLoading(false);
            }
        })();
    }, []);

    return (
        <div className="page">
            <div className="page-header">
                <h1>Clusters</h1>
            </div>

            <GalleryGrid
                photos={photos}
                loading={loading}
                onPhotoClick={(p) => navigate(`/cluster/${p.photoId}`)}
            />
        </div>
    );
};

export default ClustersPage;
