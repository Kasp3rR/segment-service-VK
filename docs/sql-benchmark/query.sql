SELECT s.id,s.code,s.name,s.description,s.created_at
 FROM users u LEFT JOIN user_segments us ON us.user_id=u.id
 LEFT JOIN segments s ON s.id=us.segment_id WHERE u.id=$1 ORDER BY us.segment_id;
